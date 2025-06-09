import os
import sys
import time
import uuid
import json
import logging
from typing import Optional, Generator
from contextlib import contextmanager
from redis import Redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 将当前目录添加到 sys.path
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from adspowerapi import AdsPowerAPI
from adspower.utils import get_redis_client, decode_bytes
from adspower.config import REDIS_KEYS, RESOURCE_MANAGEMENT, LOG_CONFIG

logger = logging.getLogger(__name__)

class ProfilePool:
    def __init__(self,
                 api: Optional[AdsPowerAPI] = None,
                 redis: Optional[Redis] = None):
        self.api = api or AdsPowerAPI()
        self.redis = redis or get_redis_client()
        self.pool_key = REDIS_KEYS["PROFILE_POOL"]
        self.heartbeat_key = REDIS_KEYS["PROCESS_HEARTBEAT"]
        self.max_pool_size = RESOURCE_MANAGEMENT["MAX_POOL_SIZE"]
        self.idle_timeout = RESOURCE_MANAGEMENT["IDLE_TIMEOUT"]
        self.lease_ttl = RESOURCE_MANAGEMENT["LEASE_TTL"]
        self.lock_prefix = "adspower:lease_lock:"
        self.sync_profiles_from_adspower()

    def _all_profiles(self):
        return {decode_bytes(k): json.loads(decode_bytes(v))
                for k, v in self.redis.hgetall(self.pool_key).items()}

    def _save_profile(self, profile):
        self.redis.hset(self.pool_key, profile["user_id"], json.dumps(profile))

    def _acquire_lock(self, user_id: str, lease_id: str) -> bool:
        lock_key = f"{self.lock_prefix}{user_id}"
        return self.redis.set(lock_key, lease_id, nx=True, ex=self.lease_ttl)

    def _release_lock(self, user_id: str, lease_id: str):
        lock_key = f"{self.lock_prefix}{user_id}"
        current = self.redis.get(lock_key)
        if current and decode_bytes(current) == lease_id:
            self.redis.delete(lock_key)

    def _mark_in_use(self, profile, spider_name):
        now = int(time.time())
        lease_id = f"{os.getpid()}_{uuid.uuid4().hex[:6]}"
        if not self._acquire_lock(profile["user_id"], lease_id):
            logger.info(f"[{spider_name}] profile {profile['user_id']} 看似空闲但锁竞争失败")
            return False
        profile.update({
            "in_use": True,
            "last_used": now,
            "lease_id": lease_id,
            "spider_name": spider_name,
        })
        self._save_profile(profile)
        return True

    def _release(self, user_id):
        raw = self.redis.hget(self.pool_key, user_id)
        if not raw:
            return
        profile = json.loads(decode_bytes(raw))
        lease_id = profile.get("lease_id")
        profile.update({
            "in_use": False,
            "lease_id": None,
            "spider_name": None,
            "last_used": int(time.time())
        })
        self._save_profile(profile)
        if lease_id:
            self._release_lock(user_id, lease_id)
            logger.info(f"释放 profile {user_id} 的锁: {lease_id}")

    def _start_browser(self, user_id):
        result = self.api.start_browser(user_id)["data"]
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", result["ws"]["selenium"])
        service = Service(executable_path=result["webdriver"])
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        return driver

    def _create_new_profile(self, spider_name):
        now = int(time.time())
        try:
            result = self.api.create_browser()
            user_id = result["data"]["id"]
        except Exception as e:
            logger.error(f"创建新 profile 失败: {e}")
            return None

        lease_id = f"{os.getpid()}_{uuid.uuid4().hex[:6]}"
        if not self._acquire_lock(user_id, lease_id):
            return None

        profile = {
            "user_id": user_id,
            "created_at": now,
            "last_used": now,
            "in_use": True,
            "is_blocked": False,
            "blocked_count": 0,
            "lease_id": lease_id,
            "spider_name": spider_name,
            "browser_opened": False
        }
        self._save_profile(profile)
        return profile

    def _get_or_create_profile(self, spider_name):
        for p in self._all_profiles().values():
            logger.info(f"检查 profile: {p['user_id']} in_use={p['in_use']} is_blocked={p['is_blocked']}")
            if not p["in_use"] and not p["is_blocked"]:
                if self._mark_in_use(p, spider_name):
                    logger.info(f"成功使用已有 profile: {p['user_id']}")
                    return p
                else:
                    logger.info(f"尝试加锁失败: {p['user_id']}")

        if len(self._all_profiles()) < self.max_pool_size:
            return self._create_new_profile(spider_name)
        
        logger.warning("已达到最大profile限制，无可用资源")
        return None

    def update_heartbeat(self):
        self.redis.hset(self.heartbeat_key, os.getpid(), int(time.time()))

    @contextmanager
    def lease(self, spider_name: str) -> Generator[webdriver.Chrome, None, None]:
        profile = self._get_or_create_profile(spider_name)
        if not profile:
            raise RuntimeError("无法租用profile")
        user_id = profile["user_id"]

        try:
            self.update_heartbeat()
            driver = self._start_browser(user_id)
            yield driver
        except Exception as e:
            err_msg = str(e)
            logger.error(f"浏览器异常: {err_msg}, user_id: {user_id}")
            if "target blocked" in err_msg or "Proxy is not exist" in err_msg:
                logger.error(f"浏览器异常并封禁: {err_msg}, user_id: {user_id}")
                self._mark_blocked(user_id)
            else:
                logger.warning(f"浏览器异常但未封禁: {err_msg}, user_id: {user_id}")
            raise
        finally:
            self._release(user_id)
            try:
                self.api.close_browser(user_id)
            except Exception as e:
                logger.warning(f"关闭浏览器失败: {e}")

    def _mark_blocked(self, user_id):
        raw = self.redis.hget(self.pool_key, user_id)
        if not raw:
            return
        profile = json.loads(decode_bytes(raw))
        profile["is_blocked"] = True
        profile["blocked_count"] = profile.get("blocked_count", 0) + 1
        if profile["blocked_count"] >= 3:
            try:
                self.api.delete_browser([user_id])
                self.redis.hdel(self.pool_key, user_id)
            except Exception as e:
                logger.error(f"删除封禁 profile 失败: {e}")
        else:
            self._save_profile(profile)

    def sync_profiles_from_adspower(self):
        """
        从 AdsPower 的实际分组同步 profile 列表到 Redis。
        如果 Redis 中缺失某个 profile，则补齐默认记录。
        """
        try:
            browser_list = self.api.get_browser_list(include_all=False)
            logger.info(f"[sync_profiles_from_adspower] 从 AdsPower 获取到 {len(browser_list)} 个 profile")

            synced = 0
            skipped = 0

            for b in browser_list:
                user_id = b.get("user_id")
                if not user_id:
                    continue

                # 如果 Redis 中已存在，跳过
                if self.redis.hexists(self.pool_key, user_id):
                    skipped += 1
                    continue

                # 构建默认 profile 数据
                profile = {
                    "user_id": user_id,
                    "created_at": int(time.time()),
                    "last_used": 0,
                    "in_use": False,
                    "is_blocked": False,
                    "blocked_count": 0,
                    "lease_id": None,
                    "spider_name": None,
                    "browser_opened": False
                }

                self._save_profile(profile)
                synced += 1
                logger.info(f"[sync_profiles_from_adspower] Redis 中缺失 profile: {user_id}，已补全")

            logger.info(f"[sync_profiles_from_adspower] 同步完成: 补全 {synced} 个，跳过 {skipped} 个")

        except Exception as e:
            logger.error(f"[sync_profiles_from_adspower] 同步失败: {str(e)}", exc_info=True)

    def cleanup(self):
        """
        清理被封、超时未释放、空闲浏览器。
        """
        now = int(time.time())
        try:
            for profile in self._all_profiles().values():
                user_id = profile["user_id"]
                in_use = profile.get("in_use", False)
                lease_id = profile.get("lease_id")
                last_used = profile.get("last_used", 0)
                is_blocked = profile.get("is_blocked", False)
                blocked_count = profile.get("blocked_count", 0)
                browser_opened = profile.get("browser_opened", False)

                # 1. 删除封号超过次数限制的 profile
                if is_blocked and blocked_count >= 3:
                    try:
                        # self.api.delete_browser([user_id])
                        # self.redis.hdel(self.pool_key, user_id)
                        logger.info(f"TODO: [cleanup] 删除被封 profile: {user_id}")
                        continue  # 该 profile 已彻底清除，无需后续处理
                    except Exception as e:
                        logger.error(f"TODO: [cleanup] 删除封号失败: {user_id}, 错误: {e}")

                # 2. 清理长时间未释放的租约
                if in_use and now - last_used > self.lease_ttl:
                    logger.info(f"[cleanup] 释放过期 profile: {user_id}, lease_id: {lease_id}")
                    try:
                        self._release(user_id)
                    except Exception as e:
                        logger.warning(f"[cleanup] 释放 profile 失败: {user_id}, 错误: {e}")

                # 3. 关闭长时间空闲的浏览器
                if not in_use and browser_opened and now - last_used > self.idle_timeout:
                    try:
                        self.api.close_browser(user_id)
                        profile["browser_opened"] = False
                        self._save_profile(profile)
                        logger.info(f"[cleanup] 关闭空闲浏览器: {user_id}")
                    except Exception as e:
                        logger.warning(f"[cleanup] 关闭浏览器失败: {user_id}, 错误: {e}")

        except Exception as e:
            logger.error(f"[cleanup] 整体清理失败: {e}", exc_info=True)


# 独立入口，仅运行 cleanup 循环
if __name__ == "__main__":
    # 使用正确的日志配置参数
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    pool = ProfilePool()

    logger.info("启动 AdsPower Profile 清理服务...")
    while True:
        try:
            pool.cleanup()
        except Exception as e:
            logger.error(f"资源清理循环异常: {e}")
        time.sleep(RESOURCE_MANAGEMENT["CLEANUP_INTERVAL"])
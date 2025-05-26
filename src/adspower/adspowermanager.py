import json
import time
import uuid
import os
import threading
import logging
from typing import Optional, Generator
from redis import Redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from adspowerapi import AdsPowerAPI  # 假设你已有 AdsPowerAPI 类

logger = logging.getLogger(__name__)


def decode_bytes(obj):
    if isinstance(obj, dict):
        return {decode_bytes(k): decode_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_bytes(i) for i in obj]
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        return obj


class AdspowerProfileLeaseManager:
    CURRENT_LEASE_ID = f"{os.getpid()}_{uuid.uuid4().hex[:6]}"

    def __init__(self, api: AdsPowerAPI, redis: Redis, lease_ttl: int = 1800):
        self.api = api
        self.redis = redis
        self.lease_ttl = lease_ttl  # 秒，默认 30 分钟
        # self.lease_id = f"{os.getpid()}_{uuid.uuid4().hex[:6]}"
        self.lease_id = AdspowerProfileLeaseManager.CURRENT_LEASE_ID
        self.profile_key = "adspower:profile_leases"
        self.user_id: Optional[str] = None
        self._renew_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def create_profile(self) -> str:
        group_id = self.api.get_or_create_random_group()
        try:
            profile = self.api.create_browser(group_id)
            user_id = profile.get("data", {}).get("id")
            if not user_id:
                raise RuntimeError(f"未获取到 user_id，create_browser 返回: {profile}")
            self.user_id = user_id
            self._register_lease()
            logger.info(f"[ProfileLease] 创建并注册指纹环境 user_id={self.user_id}, lease_id={self.lease_id}")
            return self.user_id
        except Exception as e:
            logger.error(f"[ProfileLease] 创建 profile 异常: {e}")
            if self.user_id:
                self.api.delete_browser([self.user_id])
            raise

    def _register_lease(self):
        self.redis.hset(self.profile_key, self.user_id, json.dumps({
            "lease_id": self.lease_id,
            "created_at": int(time.time()),
            "last_active": int(time.time()),
            "closed_count": 0
        }))

    def update_lease(self):
        if self.user_id:
            lease = self.redis.hget(self.profile_key, self.user_id)
            if lease:
                lease_data = json.loads(lease)
                if lease_data["lease_id"] == self.lease_id:
                    lease_data["last_active"] = int(time.time())
                    lease_data["closed_count"] = 0
                    lease_data= decode_bytes(lease_data)
                    self.redis.hset(self.profile_key, self.user_id, json.dumps(lease_data))
                    logger.info(f'[LeaseUpdate] user_id={self.user_id} lease info updated: {json.dumps(lease_data)}')

    def _start_renew_thread(self):
        def renew():
            while not self._stop_event.wait(30):  # 每 30 秒续约一次
                self.update_lease()
            # while not self._stop_event.wait(self.lease_ttl // 3):
            #     self.update_lease()
        self._renew_thread = threading.Thread(target=renew, daemon=True)
        self._renew_thread.start()

    def _stop_renew_thread(self):
        if self._renew_thread:
            self._stop_event.set()
            self._renew_thread.join()

    def start_driver(self) -> webdriver.Chrome:
        start_result = self.api.start_browser(self.user_id)
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", start_result["data"]["ws"]["selenium"])
        service = Service(executable_path=start_result["data"]["webdriver"])
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver

    def cleanup_profile(self):
        if self.user_id:
            lease = self.redis.hget(self.profile_key, self.user_id)
            if lease:
                if isinstance(lease, bytes):
                    lease = lease.decode("utf-8")
                lease_data = json.loads(lease)
                if lease_data["lease_id"] == self.lease_id:
                    logger.info(f"[ProfileLease] 正在释放 user_id={self.user_id}")
                    try:
                        self.api.close_browser(self.user_id)
                    except Exception as e:
                        logger.warning(f"关闭浏览器失败: {e}")
                    try:
                        self.api.delete_browser([self.user_id])
                        self.redis.hdel(self.profile_key, self.user_id)
                    except Exception as e:
                        logger.warning(f"删除 profile 失败: {e}")

    def __enter__(self) -> Generator[webdriver.Chrome, None, None]:
        self.create_profile()
        self._start_renew_thread()
        driver = self.start_driver()
        return driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_renew_thread()
        self.cleanup_profile()

    def start(self):
        """用于非上下文方式启动：创建 profile 并启动续约线程"""
        self.create_profile()
        self._start_renew_thread()

    def stop(self):
        """用于非上下文方式退出：停止续约 + 清理 profile"""
        self._stop_renew_thread()
        self.cleanup_profile()

    @classmethod
    def get_current_lease_id(cls):
        return cls.CURRENT_LEASE_ID

    @staticmethod
    def reclaim_expired_profiles(api: AdsPowerAPI, redis: Redis, lease_ttl: int = 1800):
        profile_key = "adspower:profile_leases"
        now = int(time.time())
        all_leases = redis.hgetall(profile_key)

        try:
            opened_profiles = api._request("GET", "/api/v1/browser/local-active")
            time.sleep(1)
            opened_users = {p["user_id"]: p for p in opened_profiles.get("data", {}).get("list", [])}
            logger.info(f"[ProfileLease] 当前活跃浏览器: {opened_users}")
        except Exception as e:
            logger.warning(f"[ProfileLease] 获取 local-active 状态失败: {e}")
            opened_users = {}

        for user_id, data in all_leases.items():
            try:
                if isinstance(user_id, bytes):
                    user_id = user_id.decode("utf-8")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                info = json.loads(data)
                lease_id = info.get("lease_id", "")
                last_active = info.get("last_active", 0)
                closed_count = info.get("closed_count", 0)

                is_running = user_id in opened_users

                # 清理过期 lease
                if now - last_active > lease_ttl:
                    logger.info(f"[ProfileLease] 释放过期 user_id={user_id}")
                    deleted = False
                    try:
                        api.close_browser(user_id)
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"关闭浏览器失败: {e}")
                    try:
                        api.delete_browser([user_id])
                        time.sleep(1)
                        deleted = True
                    except Exception as e:
                        logger.warning(f"删除 profile 失败: {e}")
                    if deleted:
                        redis.hdel(profile_key, user_id)
                    else:
                        redis.hset(profile_key, user_id, json.dumps(decode_bytes(info)))
                    continue

                # 清理孤儿进程: lease_id 不匹配且未续约超时
                if is_running and lease_id != AdspowerProfileLeaseManager.CURRENT_LEASE_ID and now - last_active > lease_ttl:
                    logger.info(f"[ProfileLease] 清理孤儿 profile user_id={user_id} lease_id={lease_id}")
                    try:
                        api.close_browser(user_id)
                    except Exception as e:
                        logger.warning(f"关闭僵尸 profile 失败: {e}")
                    try:
                        api.delete_browser([user_id])
                        redis.hdel(profile_key, user_id)
                    except Exception as e:
                        logger.warning(f"删除僵尸 profile 失败: {e}")
                    continue

                if not is_running:
                    closed_count += 1
                    logger.info(f"[ProfileLease] 浏览器关闭状态 user_id={user_id} 关闭次数={closed_count}")
                    if closed_count >= 5:
                        logger.info(f"[ProfileLease] 连续关闭 5 次，清理 user_id={user_id}")
                        deleted = False
                        try:
                            api.delete_browser([user_id])
                            deleted = True
                        except Exception as e:
                            logger.warning(f"删除 profile 失败: {e}")
                        if deleted:
                            redis.hdel(profile_key, user_id)
                        else:
                            info["closed_count"] = closed_count
                            redis.hset(profile_key, user_id, json.dumps(decode_bytes(info)))
                        continue
                    info["closed_count"] = closed_count
                    info = decode_bytes(info)
                    redis.hset(profile_key, user_id, json.dumps(info))

            except Exception as e:
                logger.warning(f"[ProfileLease] 清理 user_id={user_id} 失败: {e}")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                try:
                    redis.hset(profile_key, user_id, data)
                except Exception as inner:
                    logger.error(f"[ProfileLease] 回写 Redis 失败 user_id={user_id}: {inner}")
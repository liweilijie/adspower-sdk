import os
import sys
import signal
import time
import logging
import json
from redis import Redis
from typing import Set, Dict, List

from .config import REDIS_KEYS, RESOURCE_MANAGEMENT, SERVICE_CONFIG
from .utils import get_redis_client
from .adspowermanager import ProfilePool
from .adspowerapi import AdsPowerAPI

# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到 sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adspower.settings import SERVICE_CHECK_INTERVAL

# 配置日志
logger = logging.getLogger(__name__)

class AdsPowerCleanerService:
    """
    AdsPower资源清理服务
    定期调用ProfilePool的cleanup_resources方法清理无效资源
    
    主要功能：
    1. 定期检查和清理无效资源
    2. 同步Redis数据与实际状态
    3. 处理异常情况和数据不一致
    """
    
    def __init__(self, redis_client: Redis = None, check_interval: int = RESOURCE_MANAGEMENT["CLEANUP_INTERVAL"]):
        """
        初始化清理服务
        
        参数:
            redis_client: Redis客户端实例，如果不提供则自动创建
            check_interval: 检查间隔(秒)
        """
        self.api = AdsPowerAPI()
        self.redis = redis_client or get_redis_client()
        self.pool = ProfilePool(self.api, self.redis)
        self.check_interval = check_interval
        self.running = True
        
        # 注册信号处理
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        
        logger.info("清理服务初始化完成")

    def handle_shutdown(self, signum, frame):
        """处理进程终止信号"""
        logger.info("接收到终止信号，准备关闭清理服务...")
        self.running = False

    def _verify_redis_data(self) -> bool:
        """
        验证Redis中的数据是否完整和一致
        """
        try:
            # 检查必要的键是否存在
            for key in REDIS_KEYS.values():
                if not self.redis.exists(key):
                    logger.warning(f"Redis键 {key} 不存在")
                    return False
            
            # 检查计数是否一致
            profile_count = int(self.redis.get(REDIS_KEYS["PROFILE_COUNT"]) or 0)
            actual_count = self.redis.hlen(REDIS_KEYS["PROFILE_POOL"])
            
            if profile_count != actual_count:
                logger.warning(f"Profile计数不一致: 计数={profile_count}, 实际={actual_count}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"验证Redis数据时出错: {e}")
            return False

    def _sync_with_adspower(self) -> None:
        """
        同步Redis数据与AdsPower实际状态
        """
        try:
            # 获取AdsPower中的所有profile
            actual_profiles = self.api.get_browser_list()
            actual_profile_ids = {p["user_id"] for p in actual_profiles}
            
            # 获取Redis中的所有profile
            redis_profiles = self.pool.get_all_profiles()
            redis_profile_ids = {p["user_id"] for p in redis_profiles}
            
            # 获取当前打开的浏览器
            opened_browsers = self.api.get_opened_user_ids()
            
            # 1. 删除Redis中不存在的profile
            for profile in redis_profiles:
                if profile["user_id"] not in actual_profile_ids:
                    logger.warning(f"删除不存在的profile: {profile['user_id']}")
                    self.redis.hdel(REDIS_KEYS["PROFILE_POOL"], profile["user_id"])
            
            # 2. 添加或更新实际存在的profile
            now = int(time.time())
            for profile in actual_profiles:
                user_id = profile["user_id"]
                profile_data = {
                    "user_id": user_id,
                    "created_at": now,
                    "last_used": now,
                    "in_use": False,
                    "is_blocked": False,
                    "blocked_count": 0,
                    "lease_id": None,
                    "spider_name": None,
                    "browser_opened": user_id in opened_browsers
                }
                
                # 如果Redis中已存在，保留一些状态
                existing_data = self.redis.hget(REDIS_KEYS["PROFILE_POOL"], user_id)
                if existing_data:
                    existing = json.loads(existing_data)
                    profile_data.update({
                        "created_at": existing.get("created_at", now),
                        "is_blocked": existing.get("is_blocked", False),
                        "blocked_count": existing.get("blocked_count", 0)
                    })
                
                self.redis.hset(REDIS_KEYS["PROFILE_POOL"], user_id, json.dumps(profile_data))
            
            # 3. 更新计数
            self.redis.set(REDIS_KEYS["PROFILE_COUNT"], len(actual_profiles))
            
            logger.info(f"数据同步完成: {len(actual_profiles)} 个profile")
            
        except Exception as e:
            logger.error(f"同步数据失败: {e}")
            raise

    def cleanup_once(self) -> None:
        """执行一次资源清理"""
        try:
            # 首先验证Redis数据
            if not self._verify_redis_data():
                logger.warning("检测到Redis数据不完整或不一致，开始同步...")
                self._sync_with_adspower()
            
            # 执行常规清理
            self.pool.cleanup_resources()
            logger.info("清理任务完成")
            
        except Exception as e:
            logger.error(f"清理任务执行出错: {e}")
            # 如果是Redis连接错误，尝试重新连接
            if isinstance(e, (Redis.ConnectionError, Redis.TimeoutError)):
                logger.warning("Redis连接失败，尝试重新连接...")
                self.redis = get_redis_client()

    def run(self):
        """运行清理服务"""
        logger.info("AdsPower清理服务启动")
        
        while self.running:
            try:
                self.cleanup_once()
            except Exception as e:
                logger.error(f"清理周期执行失败: {e}")
            finally:
                time.sleep(self.check_interval)
        
        logger.info("AdsPower清理服务已停止")

def main():
    """主函数"""
    try:
        redis_client = get_redis_client()
        cleaner = AdsPowerCleanerService(redis_client=redis_client)
        cleaner.run()
    except Exception as e:
        logger.error(f"服务运行出错: {e}")
        raise

if __name__ == "__main__":
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        exit(1)

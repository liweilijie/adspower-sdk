import signal
import time
import logging
import asyncio
from redis import Redis
from adspower.adspowerapi import AdsPowerAPI
from adspower.adspowermanager import ProfilePool
from adspower.utils import get_redis_client

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class AdsPowerCleanerService:
    """
    AdsPower资源清理服务
    定期调用ProfilePool的cleanup_resources方法清理无效资源
    """
    def __init__(self, redis_client: Redis, check_interval: int = 60):  # 默认1分钟检查一次
        """
        初始化清理服务
        
        参数:
            check_interval: 检查间隔(秒)，默认60秒
        """
        self.api = AdsPowerAPI()
        self.redis = redis_client
        self.pool = ProfilePool(self.api, self.redis)
        self.check_interval = check_interval
        self.running = True
        
        # 注册信号处理
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """处理进程终止信号"""
        logger.info("接收到终止信号，准备关闭清理服务...")
        self.running = False

    async def cleanup_once(self) -> None:
        """执行一次资源清理"""
        try:
            # 检查Redis连接
            if not self.redis.ping():
                logger.error("Redis连接失败")
                return
                
            # 执行清理
            await self.pool.cleanup_resources()
            logger.info("清理任务完成")
            
        except Exception as e:
            logger.error(f"清理任务执行出错: {e}")

    async def run(self):
        """运行清理服务"""
        logger.info("AdsPower清理服务启动")
        
        while self.running:
            await self.cleanup_once()
            await asyncio.sleep(self.check_interval)

        logger.info("AdsPower清理服务已停止")

async def main():
    """主函数"""
    try:
        redis_client = get_redis_client()
        cleaner = AdsPowerCleanerService(redis_client=redis_client)
        await cleaner.run()
    except Exception as e:
        logger.error(f"服务运行出错: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        exit(1)

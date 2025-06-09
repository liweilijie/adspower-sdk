import time
import logging
import redis
from redis.exceptions import ConnectionError, TimeoutError
from adspower.config import REDIS_CONFIG

# 配置日志
logger = logging.getLogger(__name__)

class RedisPool:
    _instance = None
    
    def __init__(self):
        self._pool = None
        
    def get_pool(self):
        if self._pool is None:
            logger.info(f"redis password: {REDIS_CONFIG['PASSWORD']}")
            try:
                self._pool = redis.BlockingConnectionPool(
                    host=REDIS_CONFIG["HOST"],
                    port=REDIS_CONFIG["PORT"],
                    db=REDIS_CONFIG["DB"],
                    password=REDIS_CONFIG["PASSWORD"],
                    max_connections=REDIS_CONFIG["CONNECTIONS"],
                    # 关于 decode_responses 参数：如果您希望 Redis 返回的结果是字符串类型而不是字节串，可以将其设置为 True。这在处理字符串数据时非常方便，但如果您需要处理二进制数据（如使用 pickle 序列化的对象），建议保持为 False，以避免解码错误。
                    decode_responses=REDIS_CONFIG["DECODE_RESPONSES"],
                    health_check_interval=REDIS_CONFIG["HEALTH_CHECK_INTERVAL"],
                    timeout=30,  # 等待连接可用的最大时间（秒）
                )
            except Exception as e:
                logger.error(f"Redis连接池初始化失败: {e}")
                raise
        return self._pool
        
    def get_connection(self, max_retries: int = 3, retry_interval: int = 5):
        """
        从连接池获取一个Redis连接
        
        参数:
            max_retries: 最大重试次数，默认3次
            retry_interval: 重试间隔（秒），默认5秒
        
        返回:
            redis.StrictRedis: Redis客户端实例
            
        异常:
            redis.ConnectionError: 如果在最大重试次数内都无法连接则抛出此异常
        """
        retries = 0
        while retries < max_retries:
            try:
                pool = self.get_pool()
                client = redis.StrictRedis(connection_pool=pool)
                client.ping()  # 测试连接
                return client
                
            except (ConnectionError, TimeoutError) as e:
                retries += 1
                if retries >= max_retries:
                    logger.error(f"Redis连接失败，已达到最大重试次数({max_retries}次)")
                    raise
                    
                logger.warning(f"Redis连接失败({retries}/{max_retries})，{retry_interval}秒后重试...")
                time.sleep(retry_interval)
                
            except Exception as e:
                logger.error(f"Redis连接发生未知错误: {str(e)}")
                raise

# 创建全局单例
pool = RedisPool()

def get_redis_client(max_retries: int = 3, retry_interval: int = 5):
    """
    获取Redis客户端实例，使用全局连接池
    
    参数:
        max_retries: 最大重试次数，默认3次
        retry_interval: 重试间隔（秒），默认5秒
    
    返回:
        redis.StrictRedis: Redis客户端实例
        
    异常:
        redis.ConnectionError: 如果在最大重试次数内都无法连接则抛出此异常
    """
    return pool.get_connection(max_retries, retry_interval)


def decode_bytes(obj):
    if isinstance(obj, dict):
        return {decode_bytes(k): decode_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_bytes(i) for i in obj]
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    else:
        return obj

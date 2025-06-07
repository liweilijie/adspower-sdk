import json
import time
import uuid
import os
import sys
import threading
import logging
from typing import Optional, Generator, List, Dict, Set
from redis import Redis
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from twisted.internet import defer, reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from selenium.common.exceptions import TimeoutException, WebDriverException
import signal

# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))

# 将当前目录添加到 sys.path
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from adspowerapi import AdsPowerAPI


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


class ProfilePool:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """确保ProfilePool为单例"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance
    
    def __init__(self, api: 'AdsPowerAPI', redis: Redis, 
                 max_pool_size: int = 15,
                 idle_timeout: int = 300,
                 heartbeat_timeout: int = 60):  # 移除了 check_interval 参数
        # 防止重复初始化
        if hasattr(self, '_initialized'):
            return
            
        self.api = api
        self.redis = redis
        self.max_pool_size = max_pool_size
        self.pool_key = "adspower:profile_pool"
        self.count_key = "adspower:profile_count"
        self.heartbeat_key = "adspower:process_heartbeat"
        self.idle_timeout = idle_timeout
        self.heartbeat_timeout = heartbeat_timeout
        self._initialized = True
        
        self._init_pool_count()
        # 移除了 _start_cleanup_thread 调用

    def _init_pool_count(self):
        """初始化profile计数"""
        if not self.redis.exists(self.count_key):
            # 获取当前所有profile数量并设置
            profiles = self.get_all_profiles()
            self.redis.set(self.count_key, len(profiles))

    def _increment_pool_count(self):
        """增加profile计数"""
        return self.redis.incr(self.count_key)

    def _decrement_pool_count(self):
        """减少profile计数"""
        return self.redis.decr(self.count_key)

    def _get_pool_count(self) -> int:
        """获取当前profile总数"""
        count = self.redis.get(self.count_key)
        return int(count) if count else 0

    def cleanup_resources(self):
        """
        清理资源的核心逻辑
        现在由 AdsPowerCleanerService 调用，而不是在内部线程中运行
        """
        now = int(time.time())
        all_profiles = self.get_all_profiles()
        active_processes = self._get_active_processes()
        
        for profile in all_profiles:
            user_id = profile.get("user_id")
            if not user_id:
                continue
                
            try:
                # 检查是否需要清理已死亡进程的profile
                if profile.get("in_use"):
                    process_id = profile.get("lease_id", "").split("_")[0]
                    if process_id and process_id not in active_processes:
                        logger.warning(f"检测到死亡进程 {process_id} 的profile: {user_id}，准备清理")
                        self._cleanup_profile(user_id, profile, now)
                        continue
                
                # 检查是否需要删除被封的profile
                if profile.get("is_blocked") and profile.get("blocked_count", 0) >= 3:
                    logger.info(f"删除被封的profile: {user_id}")
                    self._delete_profile(user_id)
                    continue
                
                # 检查是否需要关闭空闲浏览器
                if (not profile.get("in_use") and 
                    profile.get("browser_opened", False) and
                    now - profile.get("last_used", 0) > self.idle_timeout):
                    logger.info(f"关闭空闲浏览器: {user_id}")
                    self._close_browser(user_id, profile)
                        
            except Exception as e:
                logger.error(f"处理profile {user_id} 时出错: {e}")

    def _cleanup_profile(self, user_id: str, profile: dict, now: int):
        """清理单个profile的资源"""
        try:
            self.api.close_browser(user_id)
        except Exception as e:
            logger.warning(f"关闭浏览器失败: {e}")
        
        profile.update({
            "in_use": False,
            "last_used": now,
            "lease_id": None,
            "spider_name": None,
            "browser_opened": False
        })
        self.redis.hset(self.pool_key, user_id, json.dumps(profile))

    def _delete_profile(self, user_id: str):
        """删除被封的profile"""
        try:
            self.api.delete_browser([user_id])
            self.redis.hdel(self.pool_key, user_id)
            self._decrement_pool_count()
        except Exception as e:
            logger.error(f"删除profile失败: {e}")

    def _close_browser(self, user_id: str, profile: dict):
        """关闭浏览器"""
        try:
            self.api.close_browser(user_id)
            profile["browser_opened"] = False
            self.redis.hset(self.pool_key, user_id, json.dumps(profile))
        except Exception as e:
            logger.warning(f"关闭浏览器失败: {e}")

    def _get_active_processes(self) -> Set[str]:
        """获取所有活跃进程的ID"""
        now = int(time.time())
        active_processes = set()
        
        # 获取所有进程心跳
        all_heartbeats = self.redis.hgetall(self.heartbeat_key)
        for pid, last_heartbeat in all_heartbeats.items():
            pid = decode_bytes(pid)
            last_heartbeat = int(decode_bytes(last_heartbeat))
            
            # 检查心跳是否超时
            if now - last_heartbeat <= self.heartbeat_timeout:
                active_processes.add(pid)
            else:
                # 清理超时的心跳记录
                self.redis.hdel(self.heartbeat_key, pid)
                
        return active_processes

    def update_process_heartbeat(self, process_id: str):
        """更新进程心跳"""
        self.redis.hset(self.heartbeat_key, process_id, int(time.time()))

    def get_available_profile(self, spider_name: str) -> Optional[str]:
        """
        获取一个可用的profile
        
        策略：
        1. 优先使用未被使用的profile
        2. 如果没有可用的且未达到上限，创建新的
        3. 如果达到上限，等待直到有profile可用
        
        Args:
            spider_name: 爬虫名称，用于跟踪哪个爬虫在使用profile
            
        Returns:
            Optional[str]: profile ID，如果没有可用的则返回None
        """
        profiles = self.get_all_profiles()
        now = int(time.time())
        
        # 先尝试找到一个未被使用且未被封禁的profile
        for profile in profiles:
            if not profile.get("in_use") and not profile.get("is_blocked"):
                user_id = profile["user_id"]
                # 更新使用状态
                profile.update({
                    "in_use": True,
                    "last_used": now,
                    "lease_id": f"{os.getpid()}_{uuid.uuid4().hex[:6]}",
                    "spider_name": spider_name
                })
                self.redis.hset(self.pool_key, user_id, json.dumps(profile))
                return user_id
        
        # 如果没有可用的且未达到上限，创建新的
        current_count = self._get_pool_count()
        if current_count < self.max_pool_size:
            try:
                group_id = self.api.get_or_create_random_group()
                result = self.api.create_browser(group_id)
                user_id = result.get("data", {}).get("id")
                if user_id:
                    profile_data = {
                        "user_id": user_id,
                        "created_at": now,
                        "last_used": now,
                        "in_use": True,
                        "is_blocked": False,
                        "blocked_count": 0,
                        "lease_id": f"{os.getpid()}_{uuid.uuid4().hex[:6]}",
                        "spider_name": spider_name,
                        "browser_opened": False
                    }
                    self.redis.hset(self.pool_key, user_id, json.dumps(profile_data))
                    self._increment_pool_count()
                    return user_id
            except Exception as e:
                logger.error(f"创建新profile失败: {e}")
        else:
            logger.warning(f"已达到profile数量上限({self.max_pool_size})，等待可用profile")
        
        return None

    def mark_profile_blocked(self, user_id: str):
        """标记profile为被封状态"""
        profile_data = self.redis.hget(self.pool_key, user_id)
        if profile_data:
            profile = json.loads(decode_bytes(profile_data))
            profile.update({
                "is_blocked": True,
                "blocked_count": profile.get("blocked_count", 0) + 1,
                "in_use": False,
                "last_used": int(time.time())
            })
            
            # 只有在被封次数达到阈值时才删除
            if profile["blocked_count"] >= 3:
                try:
                    self.api.delete_browser([user_id])
                    self.redis.hdel(self.pool_key, user_id)
                    self._decrement_pool_count()
                except Exception as e:
                    logger.error(f"删除被封profile失败: {e}")
            else:
                self.redis.hset(self.pool_key, user_id, json.dumps(profile))

    def release_profile(self, user_id: str):
        """释放profile"""
        profile_data = self.redis.hget(self.pool_key, user_id)
        if profile_data:
            profile = json.loads(decode_bytes(profile_data))
            profile.update({
                "in_use": False,
                "last_used": int(time.time()),
                "lease_id": None,
                "spider_name": None
            })
            self.redis.hset(self.pool_key, user_id, json.dumps(profile))

    def update_browser_status(self, user_id: str, is_opened: bool):
        """更新浏览器状态"""
        profile_data = self.redis.hget(self.pool_key, user_id)
        if profile_data:
            profile = json.loads(decode_bytes(profile_data))
            profile["browser_opened"] = is_opened
            self.redis.hset(self.pool_key, user_id, json.dumps(profile))

    def get_profile_info(self, user_id: str) -> Optional[Dict]:
        """获取profile信息"""
        profile_data = self.redis.hget(self.pool_key, user_id)
        if profile_data:
            return json.loads(decode_bytes(profile_data))
        return None

class AdspowerProfileLeaseManager:
    """
    Profile租用管理器
    
    这个类负责为每个Spider管理其租用的profile。
    主要功能：
    1. 从ProfilePool获取和释放profile
    2. 管理profile的生命周期
    3. 处理profile的状态检查和更新
    4. 智能管理浏览器会话
    
    使用方式：
    1. 每个Spider实例创建一个LeaseManager实例
    2. 使用with语句或start/stop方法管理profile生命周期
    3. 在检测到反爬时调用mark_current_profile_blocked
    """
    
    def __init__(self, api: 'AdsPowerAPI', redis: Redis, 
                 spider_name: str,
                 pool: Optional[ProfilePool] = None,
                 lease_ttl: int = 1800,
                 heartbeat_interval: int = 30):  # 心跳更新间隔（秒）
        self.api = api
        self.redis = redis
        self.spider_name = spider_name
        self.lease_ttl = lease_ttl
        self.pool = pool or ProfilePool(api, redis)
        self.user_id: Optional[str] = None
        self._renew_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.heartbeat_interval = heartbeat_interval
        self.process_id = str(os.getpid())

    def create_profile(self) -> str:
        """
        创建或获取一个profile
        
        策略：
        1. 从池中获取可用的profile
        2. 如果没有可用的，等待一段时间后重试
        3. 最多重试3次
        """
        retry_count = 0
        while retry_count < 3:
            user_id = self.pool.get_available_profile(self.spider_name)
            if user_id:
                self.user_id = user_id
                logger.info(f"[ProfileLease] Spider {self.spider_name} 获取到 profile: {self.user_id}")
                return self.user_id
            
            retry_count += 1
            if retry_count < 3:
                logger.info(f"等待可用profile... (重试 {retry_count}/3)")
                time.sleep(10)  # 等待10秒后重试
        
        raise RuntimeError("无法获取可用的profile，请检查pool容量设置或等待profile释放")

    def start_driver(self) -> webdriver.Chrome:
        """启动浏览器并更新状态"""
        start_result = self.api.start_browser(self.user_id)
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", start_result["data"]["ws"]["selenium"])
        service = Service(executable_path=start_result["data"]["webdriver"])
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 更新浏览器状态
        self.pool.update_browser_status(self.user_id, True)
        return driver

    def cleanup_profile(self):
        """
        清理当前profile
        
        策略：
        1. 关闭浏览器
        2. 更新浏览器状态
        3. 释放profile回池中
        """
        if self.user_id:
            try:
                self.api.close_browser(self.user_id)
                self.pool.update_browser_status(self.user_id, False)
            except Exception as e:
                logger.warning(f"关闭浏览器失败: {e}")
            self.pool.release_profile(self.user_id)
            logger.info(f"[ProfileLease] Spider {self.spider_name} 释放 profile: {self.user_id}")

    def mark_current_profile_blocked(self):
        """标记当前profile为被封状态"""
        if self.user_id:
            self.pool.mark_profile_blocked(self.user_id)
            logger.info(f"[ProfileLease] Spider {self.spider_name} 标记 profile {self.user_id} 为被封状态")

    def start(self):
        """启动profile管理"""
        self.create_profile()
        self._start_renew_thread()
        self._start_heartbeat_thread()

    def stop(self):
        """停止profile管理"""
        self._stop_renew_thread()
        self._stop_heartbeat_thread()
        self.cleanup_profile()

    def __enter__(self) -> webdriver.Chrome:
        """上下文管理器入口"""
        self.create_profile()
        self._start_renew_thread()
        self._start_heartbeat_thread()
        return self.start_driver()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self._stop_renew_thread()
        self._stop_heartbeat_thread()
        self.cleanup_profile()

    def _start_renew_thread(self):
        def renew():
            while not self._stop_event.wait(30):  # Renew every 30 seconds
                self.update_lease()
            # while not self._stop_event.wait(self.lease_ttl // 3):
            #     self.update_lease()
        self._renew_thread = threading.Thread(target=renew, daemon=True)
        self._renew_thread.start()

    def _stop_renew_thread(self):
        if self._renew_thread:
            self._stop_event.set()
            self._renew_thread.join()

    def get_user_id(self) -> Optional[str]:
        return self.user_id

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

    @inlineCallbacks
    def check_network_status(self, driver: webdriver.Chrome, target_url: str):
        """
        使用Twisted异步机制检查网络状态和反爬状态
        
        该方法使用@inlineCallbacks装饰器,允许我们在同步的Selenium操作中使用异步方式处理结果。
        虽然Selenium操作本身是同步的,但我们可以使用这种方式将它们集成到Twisted的异步流程中。
        
        检测网络状态的主要方式:
        1. 设置页面加载超时时间,检测是否能正常访问目标URL
        2. 使用JavaScript检查页面加载状态、网络错误和重定向次数
        3. 检查页面内容是否包含被封禁相关的关键词
        
        Args:
            driver: Selenium WebDriver实例
            target_url: 要访问的目标URL
            
        Returns:
            Deferred[bool]: 返回一个最终解析为布尔值的Deferred对象
                           True表示网络正常,False表示被封禁或异常
        """
        try:
            # 设置页面加载超时时间
            driver.set_page_load_timeout(10)
            driver.set_script_timeout(10)
            
            # 直接访问目标网站
            try:
                driver.get(target_url)
            except Exception as e:
                logger.warning(f"页面加载超时或失败: {e}")
                returnValue(False)
            
            # 检查页面状态
            try:
                # 使用JavaScript检查网络状态
                network_status = driver.execute_script("""
                    const performance = window.performance;
                    if (!performance) {
                        return { status: 'unknown' };
                    }
                    
                    const timing = performance.timing;
                    const navigation = performance.navigation;
                    
                    // 检查是否完全加载
                    const loadComplete = document.readyState === 'complete';
                    
                    // 检查网络错误
                    const hasNetworkError = !loadComplete && timing.loadEventEnd === 0;
                    
                    // 检查重定向次数
                    const redirectCount = navigation.redirectCount;
                    
                    return {
                        status: loadComplete ? 'complete' : 'incomplete',
                        hasNetworkError: hasNetworkError,
                        redirectCount: redirectCount
                    };
                """)
                
                if network_status.get('hasNetworkError'):
                    logger.warning("检测到网络错误")
                    returnValue(False)
                    
                if network_status.get('redirectCount', 0) > 2:
                    logger.warning("检测到异常重定向")
                    returnValue(False)
                    
            except Exception as e:
                logger.warning(f"JavaScript执行检查失败: {e}")
                returnValue(False)
            
            # 快速检查页面内容
            try:
                page_source = driver.page_source.lower()
                if any(keyword in page_source for keyword in [
                    'access denied',
                    'forbidden',
                    'blocked',
                    'security check',
                    'captcha',
                    'too many requests'
                ]):
                    logger.warning("检测到访问被拒绝")
                    returnValue(False)
            except Exception as e:
                logger.warning(f"检查页面内容失败: {e}")
                returnValue(False)
                
            returnValue(True)
            
        except Exception as e:
            logger.error(f"网络状态检查失败: {e}")
            returnValue(False)
    @inlineCallbacks
    def is_profile_blocked(self, target_url: str):
        """
        Asynchronously check if current profile is blocked
        
        This method combines API level check and actual webpage access check,
        using yield to wait for asynchronous operations to complete.
        
        Args:
            target_url: Target URL to access
            
        Returns:
            Deferred[bool]: Returns a Deferred object that eventually resolves to a boolean
                           True means blocked, False means normal
        """
        if not self.user_id:
            returnValue(False)
            
        # First check API level
        blocked = yield self.api.is_profile_blocked(self.user_id, target_url)
        if blocked:
            returnValue(True)
            
        # If API check passes, then check actual access
        try:
            driver = self.start_driver()
            try:
                status = yield self.check_network_status(driver, target_url)
                returnValue(not status)
            finally:
                driver.quit()
        except Exception as e:
            logger.error(f"Failed to check profile status: {e}")
            returnValue(True)  # If there's an exception, play it safe and assume blocked

    def _start_heartbeat_thread(self):
        """启动心跳更新线程"""
        def update_heartbeat():
            while not self._stop_event.wait(self.heartbeat_interval):
                try:
                    self.pool.update_process_heartbeat(self.process_id)
                except Exception as e:
                    logger.error(f"更新进程心跳失败: {e}")

        self._heartbeat_thread = threading.Thread(target=update_heartbeat, daemon=True)
        self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self):
        """停止心跳更新线程"""
        if self._heartbeat_thread:
            self._stop_event.set()
            self._heartbeat_thread.join()

def test_multi_spider_scenario():
    """
    测试多Spider场景下的profile共享
    """
    from redis import Redis
    from adspowerapi import AdsPowerAPI
    
    api = AdsPowerAPI()
    redis_client = Redis(host="localhost", port=6379, db=0)
    
    # 创建共享的ProfilePool
    pool = ProfilePool(api, redis_client, max_pool_size=15)  # 设置最大15个profile
    
    # 模拟多个Spider
    spider_names = ["spider1", "spider2", "spider3"]
    managers = []
    
    @inlineCallbacks
    def run_test():
        try:
            # 创建多个LeaseManager
            for spider_name in spider_names:
                manager = AdspowerProfileLeaseManager(
                    api, 
                    redis_client,
                    spider_name=spider_name,
                    pool=pool
                )
                managers.append(manager)
                
            # 测试并发获取profile
            for manager in managers:
                manager.start()
                logger.info(f"Spider {manager.spider_name} 获取到 profile: {manager.user_id}")
                
            # 等待一段时间
            time.sleep(30)
                
            # 测试profile状态
            for manager in managers:
                if manager.user_id:
                    result = yield manager.is_profile_blocked("https://www.google.com")
                    logger.info(f"Spider {manager.spider_name} profile状态: {'已封禁' if result else '正常'}")
            
            # 模拟一个profile被封
            if managers[0].user_id:
                managers[0].mark_current_profile_blocked()
                logger.info(f"模拟 Spider {managers[0].spider_name} 的profile被封")
            
            # 清理
            for manager in managers:
                manager.stop()
                
        except Exception as e:
            logger.error(f"测试过程中出错: {e}")
        finally:
            reactor.stop()
    
    # 启动测试
    reactor.callWhenRunning(run_test)
    reactor.run()

class AdsPowerCleanerService:
    """
    独立的AdsPower资源清理服务
    作为单独的进程运行，负责清理所有无效的资源
    """
    def __init__(self, check_interval: int = 60):
        self.api = AdsPowerAPI()
        self.redis = Redis(host='localhost', port=6379, db=0)
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

    def run(self):
        """运行清理服务"""
        logger.info("AdsPower清理服务启动")
        
        while self.running:
            try:
                # 直接调用 ProfilePool 的清理方法
                self.pool.cleanup_resources()
                
            except Exception as e:
                logger.error(f"清理服务运行出错: {e}")
            
            time.sleep(self.check_interval)

        logger.info("AdsPower清理服务已停止")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("=== 开始多Spider场景测试 ===")
    test_multi_spider_scenario()


# 多行注释
"""
Profile Pool 数据 (adspower:profile_pool)
# Redis HSET 结构
{
    "profile_id_1": {
        "user_id": "abcd123",
        "created_at": 1234567890,
        "last_used": 1234567890,
        "in_use": True,
        "lease_id": "hostname_1234_abc123de",  # 进程ID
        "spider_name": "my_spider_hostname_1234_abc123de",  # 唯一spider标识
        "browser_opened": True,
        "last_active": 1234567890  # 由 renew_thread 更新
    }
}

心跳数据 (adspower:process_heartbeat)
# Redis HSET 结构
{
    "hostname_1234_abc123de": "1234567890",  # 进程ID: 时间戳
    "hostname_5678_def456gh": "1234567891"   # 另一个进程的心跳
}

所以总结一下，在整个系统中有三个后台线程在工作：
租约续期线程（在 LeaseManager 中）：
每 30 秒更新一次 profile 的 last_active 时间
表明 profile 正在被使用
心跳线程（在 LeaseManager 中）：
每 30 秒更新一次进程的心跳时间
表明进程还在存活
清理线程（在 ProfilePool 中）：
每 60 秒运行一次清理检查
清理死亡进程的资源
删除被封的 profile
关闭空闲的浏览器
这三个线程互相配合，形成了一个完整的资源管理机制：
心跳线程确保进程存活状态可以被检测
租约续期确保 profile 使用状态可以被跟踪
清理线程根据心跳和租约信息，及时清理无效资源
这样的设计确保了：
资源不会被永久占用
死亡进程的资源会被及时释放
被封的 profile 会被及时清理
空闲的浏览器会被及时关闭
系统资源得到有效利用
所有这些线程都是作为守护线程（daemon=True）运行的，这意味着当主程序退出时，这些线程会自动终止，不会导致程序无法正常退出。
"""
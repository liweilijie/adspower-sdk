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
from selenium.common.exceptions import TimeoutException, WebDriverException
import signal
import redis

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


"""
ProfilePool 类分析:

1. 单例模式的必要性:
   - ProfilePool 管理着全局的 profile 资源池,需要保证所有实例共享同一个资源池
   - 避免多个实例导致的资源竞争和数据不一致
   - 确保计数器(count_key)的准确性
   - 保证清理任务不会重复执行

2. 核心职责:
   - 管理 profile 生命周期(创建、分配、释放、删除)
   - 维护 profile 使用状态
   - 处理进程心跳检测
   - 执行资源清理(被封 profile、死进程资源等)
   - 控制资源池大小

3. 关键设计:
   - 使用 Redis 存储所有状态,支持分布式部署
   - 通过心跳机制检测进程存活
   - 实现租约机制管理 profile 分配
   - 支持 profile 复用和动态扩缩容
   - 自动清理无效资源

4. 线程安全:
   - 使用 threading.Lock 保护单例创建
   - Redis 操作原子性保证并发安全
   - 状态更新通过事务保证一致性

5. 可靠性保证:
   - 异常处理和日志记录
   - 超时控制和重试机制
   - 资源泄露防护
"""

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

    def get_all_profiles(self) -> List[Dict]:
        """
        获取所有profile信息
        
        Returns:
            List[Dict]: profile列表，每个profile包含完整的信息
        """
        try:
            # 获取所有profile数据
            all_profiles = self.redis.hgetall(self.pool_key)
            
            # 解码并转换为字典列表
            profiles = []
            for user_id, profile_data in all_profiles.items():
                try:
                    # 确保user_id是字符串
                    user_id = decode_bytes(user_id)
                    # 解析profile数据
                    profile = json.loads(decode_bytes(profile_data))
                    # 确保user_id存在于profile中
                    profile["user_id"] = user_id
                    profiles.append(profile)
                except json.JSONDecodeError as e:
                    logger.error(f"解析profile数据失败 {user_id}: {e}")
                except Exception as e:
                    logger.error(f"处理profile {user_id} 时出错: {e}")
                    
            return profiles
            
        except Exception as e:
            logger.error(f"获取profile列表失败: {e}")
            return []

    def _sync_with_adspower(self) -> None:
        """
        同步 Redis 中的数据与 AdsPower 实际数据
        在 Redis 数据丢失或不一致时调用
        """
        try:
            # 获取 AdsPower 中的所有 profile
            actual_profiles = self.api.get_browser_list()
            actual_profile_ids = {p["user_id"] for p in actual_profiles}
            
            # 获取 Redis 中的所有 profile
            redis_profiles = self.get_all_profiles()
            redis_profile_ids = {p["user_id"] for p in redis_profiles}
            
            # 检查是否需要同步
            if not redis_profiles or actual_profile_ids != redis_profile_ids:
                logger.warning("检测到 Redis 数据与实际不一致，开始同步...")
                
                # 获取当前活跃的浏览器
                opened_profiles = self.api.get_opened_user_ids()
                
                # 重建 Redis 数据
                now = int(time.time())
                for profile in actual_profiles:
                    user_id = profile["user_id"]
                    profile_data = {
                        "user_id": user_id,
                        "created_at": now,
                        "last_used": now,
                        "in_use": user_id in opened_profiles,
                        "is_blocked": False,
                        "blocked_count": 0,
                        "lease_id": None,
                        "spider_name": None,
                        "browser_opened": user_id in opened_profiles,
                        "group_id": profile.get("group_id")
                    }
                    self.redis.hset(self.pool_key, user_id, json.dumps(profile_data))
                
                # 更新计数
                self.redis.set(self.count_key, len(actual_profiles))
                logger.info(f"数据同步完成，共同步了 {len(actual_profiles)} 个 profile")
                
        except Exception as e:
            logger.error(f"同步数据失败: {e}")

    def cleanup_resources(self):
        """
        清理资源的核心逻辑
        现在由 AdsPowerCleanerService 调用，而不是在内部线程中运行
        """
        try:
            # 首先检查并同步数据
            self._sync_with_adspower()
            
            now = int(time.time())
            all_profiles = self.get_all_profiles()
            active_processes = self._get_active_processes()
            
            logger.info(f"当前活跃进程: {active_processes}")
            logger.info(f"所有profiles状态: {json.dumps(all_profiles, indent=2)}")
            
            for profile in all_profiles:
                user_id = profile.get("user_id")
                if not user_id:
                    continue
                    
                try:
                    # 检查是否需要清理已死亡进程的profile
                    if profile.get("in_use") or profile.get("browser_opened"):
                        lease_id = profile.get("lease_id", "")
                        process_id = lease_id.split("_")[0] if lease_id else None
                        
                        if process_id and process_id not in active_processes:
                            logger.warning(f"检测到死亡进程 {process_id} 的profile: {user_id}，准备清理")
                            self._cleanup_profile(user_id, profile, now)
                            continue
                        elif not process_id and profile.get("browser_opened"):
                            logger.warning(f"检测到无进程信息但浏览器打开的profile: {user_id}，准备清理")
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
                    
        except Exception as e:
            logger.error(f"清理资源时发生错误: {e}")
            # 如果是 Redis 连接错误，可以选择重试或者报警
            if isinstance(e, (redis.ConnectionError, redis.TimeoutError)):
                logger.critical("Redis 连接失败，请检查 Redis 服务状态")

    def _cleanup_profile(self, user_id: str, profile: dict, now: int):
        """清理单个profile的资源"""
        try:
            # 先检查浏览器是否真的在运行
            is_active = self.api.is_browser_active(user_id)
            if is_active:
                logger.info(f"关闭浏览器 {user_id}")
                self.api.close_browser(user_id)
            
            # 更新状态
            profile.update({
                "in_use": False,
                "last_used": now,
                "lease_id": None,
                "spider_name": None,
                "browser_opened": False
            })
            self.redis.hset(self.pool_key, user_id, json.dumps(profile))
            logger.info(f"已清理profile {user_id} 的状态")
        except Exception as e:
            logger.error(f"清理profile {user_id} 失败: {e}")

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
        logger.info(f"所有进程心跳: {all_heartbeats}")
        
        for pid, last_heartbeat in all_heartbeats.items():
            pid = decode_bytes(pid)
            last_heartbeat = int(decode_bytes(last_heartbeat))
            
            # 检查心跳是否超时
            if now - last_heartbeat <= self.heartbeat_timeout:
                active_processes.add(pid)
                logger.debug(f"进程 {pid} 活跃中，最后心跳: {last_heartbeat}")
            else:
                # 清理超时的心跳记录
                logger.warning(f"进程 {pid} 心跳超时，最后心跳: {last_heartbeat}")
                self.redis.hdel(self.heartbeat_key, pid)
                
        return active_processes

    def update_process_heartbeat(self, process_id: str):
        """更新进程心跳"""
        self.redis.hset(self.heartbeat_key, process_id, int(time.time()))

    def get_available_profile(self, spider_name: str, group_id: Optional[str] = None) -> Optional[str]:
        """
        获取一个可用的profile
        
        策略：
        1. 优先使用未被使用的profile
        2. 如果没有可用的且未达到上限，创建新的
        3. 如果达到上限，等待直到有profile可用
        
        Args:
            spider_name: 爬虫名称，用于跟踪哪个爬虫在使用profile
            group_id: 组ID，用于区分不同的爬虫组
            
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
                    "spider_name": spider_name,
                    "group_id": group_id
                })
                self.redis.hset(self.pool_key, user_id, json.dumps(profile))
                return user_id
        
        # 如果没有可用的且未达到上限，创建新的
        current_count = self._get_pool_count()
        if current_count < self.max_pool_size:
            try:
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
                        "browser_opened": False,
                        "group_id": group_id
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
                 heartbeat_interval: int = 30):
        self.api = api
        self.redis = redis
        self.spider_name = spider_name
        self.pool = pool or ProfilePool(api, redis)
        self.user_id: Optional[str] = None
        self._renew_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self.heartbeat_interval = heartbeat_interval
        self.process_id = str(os.getpid())
        self.lease_id = None  # 添加lease_id属性
        self.profile_key = "adspower:profile_pool"  # 添加profile_key属性
        logger.info(f"✅ process_id: {self.process_id}, spider_name: {self.spider_name}")

    def create_profile(self, group_id: Optional[str] = None) -> str:
        """创建或获取一个profile"""
        retry_count = 0
        while retry_count < 3:
            user_id = self.pool.get_available_profile(self.spider_name, group_id)
            if user_id:
                self.user_id = user_id
                self.lease_id = f"{self.process_id}_{uuid.uuid4().hex[:6]}"  # 设置lease_id
                logger.info(f"[ProfileLease] Spider {self.spider_name} 获取到 profile: {self.user_id}")
                return self.user_id
            
            retry_count += 1
            if retry_count < 3:
                logger.info(f"等待可用profile... (重试 {retry_count}/3)")
                time.sleep(10)
        
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

    def start(self, group_id: Optional[str] = None):
        """启动profile管理"""
        self.create_profile(group_id)
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
        """更新租约信息"""
        if not self.user_id or not self.lease_id:
            return
            
        try:
            lease = self.redis.hget(self.profile_key, self.user_id)
            if lease:
                lease_data = json.loads(decode_bytes(lease))
                if lease_data.get("lease_id") == self.lease_id:
                    lease_data.update({
                        "last_active": int(time.time()),
                        "closed_count": 0
                    })
                    self.redis.hset(self.profile_key, self.user_id, json.dumps(lease_data))
                    logger.info(f'[LeaseUpdate] user_id={self.user_id} lease info updated')
        except Exception as e:
            logger.error(f"更新租约失败: {e}")

    def check_network_status(self, driver: webdriver.Chrome, target_url: str) -> bool:
        """
        同步方式检查网络状态和反爬状态
        
        检测网络状态的主要方式:
        1. 设置页面加载超时时间,检测是否能正常访问目标URL
        2. 使用JavaScript检查页面加载状态、网络错误和重定向次数
        3. 检查页面内容是否包含被封禁相关的关键词
        
        Args:
            driver: Selenium WebDriver实例
            target_url: 要访问的目标URL
            
        Returns:
            bool: True表示网络正常,False表示被封禁或异常
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
                return False
            
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
                    return False
                    
                if network_status.get('redirectCount', 0) > 2:
                    logger.warning("检测到异常重定向")
                    return False
                    
            except Exception as e:
                logger.warning(f"JavaScript执行检查失败: {e}")
                return False
            
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
                    return False
            except Exception as e:
                logger.warning(f"检查页面内容失败: {e}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"网络状态检查失败: {e}")
            return False

    def is_profile_blocked(self, target_url: str) -> bool:
        """
        同步方式检查当前profile是否被封禁
        
        这个方法结合了API级别的检查和实际的网页访问检查
        
        Args:
            target_url: 目标URL
            
        Returns:
            bool: True表示被封禁，False表示正常
        """
        if not self.user_id:
            return False
            
        # 首先检查API级别
        try:
            blocked = self.api.is_profile_blocked(self.user_id, target_url)
            if blocked:
                return True
        except Exception as e:
            logger.error(f"API检查失败: {e}")
            return True  # 如果API检查失败，为安全起见认为被封
            
        # 如果API检查通过，则检查实际访问
        try:
            driver = self.start_driver()
            try:
                return not self.check_network_status(driver, target_url)
            finally:
                driver.quit()
        except Exception as e:
            logger.error(f"检查profile状态失败: {e}")
            return True  # 如果有异常，为安全起见认为被封

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
import logging
import time
from contextlib import contextmanager
from typing import Optional, Generator
from redis import Redis
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from .adspowermanager import AdspowerProfileLeaseManager
from .adspowerapi import AdsPowerAPI

logger = logging.getLogger(__name__)

class AdsPowerMixin:
    """
    AdsPower Mixin类
    提供了便捷的浏览器管理功能
    
    特性:
    1. 自动管理浏览器生命周期
    2. 智能处理反爬和异常
    3. 提供便捷的浏览器操作方法
    
    使用示例:
    ```python
    from scrapy_redis.spiders import RedisSpider
    
    class YourSpider(RedisSpider, AdsPowerMixin):
        name = 'your_spider'
        
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.init_adspower(**kwargs)  # 初始化AdsPower功能
            
        def parse(self, response):
            with self.get_browser() as driver:
                driver.get(response.url)
                # ... 处理页面 ...
    ```
    """
    
    def init_adspower(self, redis=None, spider_name=None, **kwargs):
        """
        初始化AdsPower功能
        在Spider的__init__方法中调用
        
        参数:
            redis: Redis客户端实例
            spider_name: 爬虫名称,如果不传则使用self.name
            heartbeat_interval: 心跳间隔（秒）
        """
        # 从kwargs中获取配置
        heartbeat_interval = int(kwargs.get('heartbeat_interval', 30))
        
        # 初始化API
        self.api = AdsPowerAPI()
        
        # 使用传入的redis客户端
        self.redis = redis

        logger.info(f"redis: {self.redis}")
        
        # 创建LeaseManager
        self.lease_manager = AdspowerProfileLeaseManager(
            api=self.api,
            redis=self.redis,
            spider_name=spider_name or self.name,
            heartbeat_interval=heartbeat_interval
        )

        logger.info(f"spider_name: {spider_name}")
        
        # 当前使用的profile信息
        self._current_profile = None
    @contextmanager
    def get_browser(self, group_name: Optional[str] = None, 
                   page_load_timeout: int = 60,
                   implicit_wait: int = 10) -> Generator[webdriver.Chrome, None, None]:
        """
        获取一个浏览器实例的上下文管理器
        
        参数:
            group_name: 指定使用的AdsPower分组名称，如果提供，会从指定分组获取profile
            page_load_timeout: 页面加载超时时间（秒）
            implicit_wait: 隐式等待时间（秒）
            
        用法:
            with self.get_browser() as driver:
                driver.get("https://example.com")
                # ... 处理页面 ...
        """
        try:
            # 如果指定了分组，先确保分组存在
            if group_name:
                group_id = self.api.get_or_create_group_by_name(group_name)
                # 从指定分组获取profile
                self.lease_manager.start(group_id)
            else:
                # 使用默认分组
                self.lease_manager.start()
            
            # 启动浏览器
            driver = self.lease_manager.start_driver()
            if not driver:
                raise RuntimeError("无法启动浏览器")
                
            # 配置浏览器
            driver.set_page_load_timeout(page_load_timeout)
            driver.implicitly_wait(implicit_wait)
            
            # 返回浏览器实例
            yield driver
            
        except Exception as e:
            logger.error(f"浏览器管理出错: {e}")
            # 检查是否是反爬相关的错误
            if any(keyword in str(e).lower() for keyword in [
                "blocked", "forbidden", "captcha", "verify", "challenge"
            ]):
                self.mark_profile_blocked()
            raise
            
        finally:
            # 确保资源被正确释放
            self.lease_manager.stop()
            
    def mark_profile_blocked(self):
        """标记当前profile为被封状态"""
        self.lease_manager.mark_current_profile_blocked()
        
    def wait_for_element(self, driver: webdriver.Chrome, by, value: str, 
                        timeout: int = 10, condition=EC.presence_of_element_located):
        """
        等待元素出现
        
        参数:
            driver: WebDriver实例
            by: 定位方式（如By.ID, By.XPATH等）
            value: 定位值
            timeout: 超时时间（秒）
            condition: 等待条件（默认为元素出现）
            
        返回:
            找到的元素
        """
        try:
            wait = WebDriverWait(driver, timeout)
            element = wait.until(condition((by, value)))
            return element
        except TimeoutException:
            logger.warning(f"等待元素超时: {by}={value}")
            raise
            
    def scroll_page(self, driver: webdriver.Chrome, 
                    pause_time: float = 0.5,
                    scroll_increment: int = 300,
                    max_retries: int = 3):
        """
        智能滚动页面
        
        参数:
            driver: WebDriver实例
            pause_time: 每次滚动后的暂停时间（秒）
            scroll_increment: 每次滚动的像素增量
            max_retries: 最大重试次数（高度不变时）
        """
        last_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0
        unchanged_count = 0
        
        while current_position < last_height and unchanged_count < max_retries:
            # 增加滚动位置
            current_position += scroll_increment
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            time.sleep(pause_time)
            
            # 检查页面高度是否变化
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                unchanged_count += 1
            else:
                unchanged_count = 0
                last_height = new_height
                
    def safe_click(self, driver: webdriver.Chrome, element,
                   retry_count: int = 3,
                   retry_delay: float = 0.5):
        """
        安全地点击元素，处理常见的点击问题
        
        参数:
            driver: WebDriver实例
            element: 要点击的元素
            retry_count: 重试次数
            retry_delay: 重试间隔（秒）
        """
        from selenium.webdriver.common.action_chains import ActionChains
        
        for attempt in range(retry_count):
            try:
                # 尝试常规点击
                element.click()
                return
            except Exception as e:
                if attempt == retry_count - 1:
                    raise
                    
                logger.warning(f"点击失败，尝试其他方式: {e}")
                try:
                    # 尝试使用JavaScript点击
                    driver.execute_script("arguments[0].click();", element)
                    return
                except:
                    # 尝试使用ActionChains点击
                    try:
                        ActionChains(driver).move_to_element(element).click().perform()
                        return
                    except:
                        time.sleep(retry_delay)
                        continue
                        
    def cleanup_adspower(self):
        """
        清理AdsPower资源
        在Spider的close方法中调用
        """
        try:
            self.lease_manager.stop()
        except Exception as e:
            logger.error(f"关闭资源时出错: {e}") 
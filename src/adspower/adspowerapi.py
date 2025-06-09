import sys
import httpx
import time
import random
from typing import List, Optional, Dict, Union, Literal
import logging
import socket
from urllib.parse import urlparse, urljoin
import os
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 获取当前文件的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

# 将父目录添加到 sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from adspower.config import SPIDER_GROUP_NAME, FINGERPRINT_CONFIG, PROXY_CONFIG

logger = logging.getLogger(__name__)

class AdsPowerAPI:
    def __init__(self, base_url: str = "http://local.adspower.net:50325"):
        self.base_url = base_url.rstrip('/')
        self.endpoints = {
            "create_browser": "/api/v1/user/create",
            "start_browser": "/api/v1/browser/start",
            "close_browser": "/api/v1/browser/stop",  # 已确认的关闭浏览器接口
            "delete_browser": "/api/v1/user/delete",
            "get_browser_list": "/api/v1/user/list",
            "create_group": "/api/v1/group/create",
            "get_group_list": "/api/v1/group/list",
            "get_active_browsers": "/api/v1/browser/active"
        }
        self._spider_group_id = None
        self._network_cache = {}
        self._cache_ttl = 30  # 缓存有效期30秒
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 最大重试次数
            backoff_factor=0.5,  # 重试间隔
            status_forcelist=[500, 502, 503, 504],  # 需要重试的HTTP状态码
        )
        
        # 创建会话并配置重试
        self.session = requests.Session()
        self.session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def _get_cached_network_status(self, key: str) -> Optional[bool]:
        """从缓存中获取网络状态"""
        if key in self._network_cache:
            status, timestamp = self._network_cache[key]
            if time.time() - timestamp < self._cache_ttl:
                return status
            else:
                del self._network_cache[key]
        return None

    def _set_cached_network_status(self, key: str, status: bool):
        """设置网络状态缓存"""
        self._network_cache[key] = (status, time.time())

    def _check_single_url(self, url: str, timeout: int = 5) -> bool:
        """同步检查单个URL的可访问性"""
        try:
            with httpx.Client(timeout=timeout) as client:
                response = client.head(url)
                return response.status_code == 200
        except Exception:
            return False

    def check_network_connectivity(self, url: str, timeout: int = 5) -> bool:
        """
        同步方式检查网络连通性
        
        Args:
            url: 要检查的目标URL
            timeout: 超时时间（秒）
            
        Returns:
            bool: True表示网络正常，False表示异常
        """
        # 先检查缓存
        cache_key = f"network_status_{url}"
        cached_status = self._get_cached_network_status(cache_key)
        if cached_status is not None:
            return cached_status

        try:
            # 检查多个URL
            test_urls = [
                "https://www.google.com",
                "https://www.bing.com",
                url
            ]
            
            # 同步执行检查
            results = []
            for test_url in test_urls:
                result = self._check_single_url(test_url, timeout=timeout)
                results.append(result)

            # 如果能访问通用网站但不能访问目标网站，可能是被封
            status = True
            if any(results[:2]) and not results[2]:
                status = False

            # 更新缓存
            self._set_cached_network_status(cache_key, status)
            return status

        except Exception as e:
            logger.warning(f"网络连通性检查失败: {e}")
            return False

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """
        发送HTTP请求到AdsPower API
        
        参数:
            method: HTTP方法 ('GET' 或 'POST')
            endpoint: API端点
            **kwargs: 请求参数
            
        返回:
            dict: API响应
            
        异常:
            requests.RequestException: 当请求失败时
            ValueError: 当响应格式无效时
        """
        url = f"{self.base_url}{endpoint}"
        timeout = kwargs.pop('timeout', (5, 15))  # 连接超时5秒，读取超时15秒
        
        try:
            response = requests.request(
                method=method,
                url=url,
                timeout=timeout,
                **kwargs
            )
            
            # 确保响应是JSON格式
            try:
                result = response.json()
            except ValueError:
                logger.error(f"API响应不是有效的JSON格式: {response.text}")
                raise ValueError("API响应格式无效")
            
            # 检查API响应状态
            if result.get('code') != 0:
                error_msg = result.get('msg', '未知错误')
                logger.error(f"API调用失败: {error_msg}")
                raise ValueError(f"API错误: {error_msg}")
            
            # 强制等待1秒，确保不会超过请求频率限制
            time.sleep(1)
            
            return result
            
        except requests.Timeout:
            logger.error(f"请求超时: {url}")
            raise
        except requests.ConnectionError:
            logger.error(f"连接失败: {url}")
            raise
        except Exception as e:
            logger.error(f"请求失败: {url}, 错误: {str(e)}")
            raise

    def get_or_create_spider_group(self) -> str:
        """获取或创建固定的爬虫分组"""
        if self._spider_group_id:
            return self._spider_group_id

        try:
            # 获取所有分组
            result = self._request("GET", self.endpoints["get_group_list"])
            groups = result.get("data", {}).get("list", [])
            logger.debug(f"获取到的分组列表: {groups}")
            
            # 查找爬虫分组
            for group in groups:
                if group.get("group_name") == SPIDER_GROUP_NAME:
                    self._spider_group_id = group["group_id"]
                    logger.info(f"找到现有爬虫分组: {SPIDER_GROUP_NAME}, ID: {self._spider_group_id}")
                    return self._spider_group_id
            
            # 如果不存在则创建
            logger.info(f"未找到爬虫分组 {SPIDER_GROUP_NAME}，准备创建...")
            create_payload = {"group_name": SPIDER_GROUP_NAME}
            logger.debug(f"创建分组请求参数: {create_payload}")
            
            result = self._request(
                "POST",
                self.endpoints["create_group"],
                json=create_payload
            )
            
            self._spider_group_id = result.get("data", {}).get("group_id")
            
            if not self._spider_group_id:
                error_msg = f"创建分组失败，API返回: {result}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            logger.info(f"成功创建爬虫分组: {SPIDER_GROUP_NAME}, ID: {self._spider_group_id}")
            return self._spider_group_id
            
        except Exception as e:
            error_msg = f"获取或创建爬虫分组失败: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def create_browser(self, name: Optional[str] = None) -> dict:
        """
        在固定的爬虫分组中创建浏览器配置。
        
        如果配置了代理，则使用代理配置；
        否则显式设置不使用代理（proxy_soft = no_proxy）。
        """
        try:
            group_id = self.get_or_create_spider_group()
            if not group_id:
                raise Exception("无法获取或创建爬虫分组")

            # 构建请求 payload
            payload = {
                "group_id": group_id,
                "name": name or f"spider_profile_{int(time.time())}",
                "fingerprint_config": FINGERPRINT_CONFIG,
            }

            # 设置代理配置
            if PROXY_CONFIG and PROXY_CONFIG.get("proxy_soft") != "no_proxy":
                payload["user_proxy_config"] = PROXY_CONFIG
            else:
                # 更稳定地声明不使用代理
                payload["user_proxy_config"] = {"proxy_soft": "no_proxy"}

            logger.info(f"创建浏览器，参数: {payload}")
            result = self._request("POST", self.endpoints["create_browser"], json=payload)

            user_id = result.get("data", {}).get("id")
            if not user_id:
                raise Exception(f"创建浏览器失败，API返回: {result}")

            logger.info(f"成功创建浏览器，ID: {user_id}")
            return result

        except Exception as e:
            error_msg = f"创建浏览器失败: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def start_browser(self, user_id: str) -> dict:
        """
        启动浏览器
        
        Args:
            user_id: 环境ID，创建环境成功后生成的唯一ID
            
        Returns:
            dict: {
                "ws": {
                    "selenium": "127.0.0.1:xxxx",    # 浏览器debug接口，用于selenium自动化
                    "puppeteer": "ws://127.0.0.1:xxxx/devtools/browser/xxxxxx"   # 浏览器debug接口，用于puppeteer自动化
                },
                "debug_port": "xxxx",  # debug端口
                "webdriver": "C:\\xxxx\\chromedriver.exe"  # webdriver路径
            }
            
        Docs:
            https://localapi-doc-zh.adspower.net/docs/Exqfpw
        """
        if not user_id:
            raise ValueError("user_id 不能为空")
            
        try:
            # 设置启动参数
            params = {
                "user_id": user_id,
                "open_tabs": 1,  # 1: 不打开平台和历史页面
                "ip_tab": 0,     # 0: 不打开ip检测页
            }
            
            result = self._request("GET", self.endpoints["start_browser"], params=params)
            logger.info(f"启动浏览器成功: {user_id}")
            return result
            
        except Exception as e:
            error_msg = f"启动浏览器失败: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def close_browser(self, user_id: str) -> dict:
        """
        关闭浏览器
        
        参数:
            user_id: 环境ID，创建环境成功后生成的唯一ID
            
        返回:
            dict: API响应结果
            
        文档:
            https://localapi-doc-zh.adspower.net/docs/ZwjVVH
        """
        if not user_id:
            raise ValueError("user_id 不能为空")
            
        try:
            result = self._request("GET", self.endpoints["close_browser"], params={"user_id": user_id})
            logger.info(f"关闭浏览器成功: {user_id}")
            return result
        except Exception as e:
            error_msg = f"关闭浏览器失败: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def delete_browser(self, user_ids: List[str]) -> dict:
        """
        删除浏览器环境
        
        Args:
            user_ids: 要删除的浏览器ID列表，一次最多100个
            
        Returns:
            dict: API响应结果
        """
        try:
            if not user_ids:
                raise ValueError("user_ids 不能为空")
                
            if len(user_ids) > 100:
                raise ValueError("一次最多只能删除100个环境")
                
            # 确保所有ID都是字符串类型
            user_ids = [str(uid) for uid in user_ids]
            
            payload = {"user_ids": user_ids}
            logger.debug(f"删除浏览器，参数: {payload}")
            
            result = self._request("POST", self.endpoints["delete_browser"], json=payload)
            logger.info(f"成功删除 {len(user_ids)} 个浏览器环境")
            return result
            
        except Exception as e:
            error_msg = f"删除浏览器失败: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

    def get_opened_user_ids(self) -> set:
        """获取所有打开的浏览器ID"""
        resp = self._request("GET", "/api/v1/browser/local-active")
        if resp and resp.get("code") == 0:
            return set(item["user_id"] for item in resp["data"].get("list", []))
        return set()

    def is_browser_active(self, user_id: str) -> bool:
        """检查浏览器是否活跃"""
        try:
            local_users = self.get_opened_user_ids()
            logger.debug(f'local_users: {local_users}')
            return user_id in local_users
        except Exception as e:
            logger.warning(f"使用 local-active 检查失败，降级为 /active: {e}")
            resp = self._request("GET", "/api/v1/browser/active", params={"user_id": user_id})
            return resp.get("code") == 0 and resp.get("data", {}).get("status") == "Active"

    def get_browser_list(self, include_all: bool = False) -> List[dict]:
        """获取浏览器列表"""
        try:
            params = {
                "page": 1,
                "page_size": 100  # 设置较大的页面大小以获取所有数据
            }
            
            if not include_all:
                # 只获取爬虫分组的浏览器
                group_id = self.get_or_create_spider_group()
                if not group_id:
                    raise Exception("无法获取或创建爬虫分组")
                params["group_id"] = group_id
                
            logger.debug(f"获取浏览器列表，参数: {params}")
            resp = self._request("GET", self.endpoints["get_browser_list"], params=params)
            browser_list = resp.get("data", {}).get("list", [])
            logger.debug(f"获取到 {len(browser_list)} 个浏览器")
            return browser_list
            
        except Exception as e:
            logger.error(f"获取浏览器列表失败: {str(e)}")
            raise

    def get_group_list(self) -> List[dict]:
        resp = self._request("GET", self.endpoints["get_group_list"])
        return resp.get("data", {}).get("list", [])

    def get_or_create_group_by_name(self, group_name: str = "spider") -> str:
        """
        根据组名获取或创建组
        
        Args:
            group_name: 组名，默认为"spider"
            
        Returns:
            str: 组ID
        """
        # 先查找是否存在同名组
        groups = self.get_group_list()
        for group in groups:
            if group.get("group_name") == group_name:
                return str(group["group_id"])
        
        # 不存在则创建新组
        resp = self._request("POST", self.endpoints["create_group"], json={"group_name": group_name})
        return str(resp["data"]["group_id"])

    def get_or_create_random_group(self) -> str:
        groups = self.get_group_list()
        if groups:
            return str(random.choice(groups)["group_id"])
        else:
            group_name = f"auto_{int(time.time())}"
            resp = self._request("POST", self.endpoints["create_group"], json={"group_name": group_name})
            return str(resp["data"]["group_id"])

    def is_profile_blocked(self, user_id: str, target_url: str) -> bool:
        """
        同步方式检查profile是否被封
        
        Args:
            user_id: profile ID
            target_url: 目标URL
            
        Returns:
            bool: True表示被封，False表示正常
        """
        try:
            # 先检查浏览器是否正常运行
            if not self.is_browser_active(user_id):
                return False
                
            # 检查网络连通性
            status = self.check_network_connectivity(target_url)
            return not status
                
        except Exception as e:
            logger.error(f"检查profile状态失败: {e}")
            return True  # 如有异常，保守起见认为被封

def test_network_check():
    """测试网络检测功能"""
    api = AdsPowerAPI()
    
    try:
        # 测试可访问的URL
        logger.info("测试 google.com ...")
        result = api.check_network_connectivity("https://www.google.com")
        logger.info(f"Google可访问性: {result}")
        
        # 测试不可访问的URL
        logger.info("测试无效URL...")
        result = api.check_network_connectivity("https://invalid.example.com")
        logger.info(f"无效URL可访问性: {result}")
        
        # 测试缓存
        logger.info("测试缓存...")
        result1 = api.check_network_connectivity("https://www.google.com")
        result2 = api.check_network_connectivity("https://www.google.com")
        logger.info(f"缓存测试结果: {result1 == result2}")
        
    except Exception as e:
        logger.error(f"测试过程中出错: {e}")

def test_profile_block_check():
    """测试profile封禁检测功能"""
    api = AdsPowerAPI()
    
    try:
        # 创建一个测试profile
        group_id = api.get_or_create_random_group()
        profile = api.create_browser(group_id)
        user_id = profile.get("data", {}).get("id")
        
        if not user_id:
            logger.error("创建profile失败")
            return
            
        logger.info(f"创建测试profile: {user_id}")
        
        # 测试profile状态检查
        logger.info("检查profile状态...")
        result = api.is_profile_blocked(user_id, "https://www.google.com")
        logger.info(f"Profile状态检查结果: {'已封禁' if result else '正常'}")
        
        # 清理测试profile
        api.delete_browser([user_id])
        logger.info("测试完成，已清理测试profile")
        
    except Exception as e:
        logger.error(f"测试过程中出错: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # 运行测试
    logger.info("=== 开始网络检测测试 ===")
    test_network_check()
    
    logger.info("\n=== 开始profile状态检测测试 ===")
    test_profile_block_check()

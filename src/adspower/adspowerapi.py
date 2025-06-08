import httpx
import time
import random
from typing import List, Optional, Dict, Union, Literal
import logging
import socket
from urllib.parse import urlparse
import os
import json
import requests
from .config import SPIDER_GROUP_NAME, FINGERPRINT_CONFIG, PROXY_CONFIG
from .exceptions import AdsPowerException, ProfileNotFoundError

logger = logging.getLogger(__name__)

class AdsPowerAPI:
    def __init__(self, base_url: str = "http://local.adspower.net:50325"):
        self.base_url = base_url.rstrip('/')
        self.endpoints = {
            "create_browser": "/api/v1/browser/create",
            "start_browser": "/api/v1/browser/start",
            "close_browser": "/api/v1/browser/stop",
            "delete_browser": "/api/v1/browser/delete",
            "get_browser_list": "/api/v1/browser/list",
            "create_group": "/api/v1/group/create",
            "get_group_list": "/api/v1/group/list"
        }
        self._spider_group_id = None
        self._network_cache = {}
        self._cache_ttl = 30  # 缓存有效期30秒

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
        """发送HTTP请求到AdsPower API"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            result = response.json()
            
            if result.get("code") != 0:
                logger.error(f"API请求失败: {result.get('msg')}")
                raise Exception(result.get("msg"))
                
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"请求失败: {e}")
            raise

    def get_or_create_spider_group(self) -> str:
        """获取或创建固定的爬虫分组"""
        if self._spider_group_id:
            return self._spider_group_id

        # 获取所有分组
        groups = self._request("GET", self.endpoints["get_group_list"]).get("data", {}).get("list", [])
        
        # 查找爬虫分组
        for group in groups:
            if group.get("name") == SPIDER_GROUP_NAME:
                self._spider_group_id = group["group_id"]
                return self._spider_group_id
        
        # 如果不存在则创建
        result = self._request(
            "POST",
            self.endpoints["create_group"],
            json={"name": SPIDER_GROUP_NAME}
        )
        self._spider_group_id = result.get("data", {}).get("group_id")
        
        if not self._spider_group_id:
            raise Exception("创建爬虫分组失败")
            
        return self._spider_group_id

    def create_browser(self, name: Optional[str] = None) -> dict:
        """在固定的爬虫分组中创建浏览器"""
        group_id = self.get_or_create_spider_group()
        
        payload = {
            "group_id": group_id,
            "name": name or f"spider_profile_{int(time.time())}",
            "user_proxy_config": PROXY_CONFIG,
            "fingerprint_config": FINGERPRINT_CONFIG
        }
        return self._request("POST", self.endpoints["create_browser"], json=payload)

    def start_browser(self, user_id: str) -> dict:
        """启动浏览器"""
        if not user_id:
            raise ValueError("user_id 不可为空")
        return self._request("GET", self.endpoints["start_browser"], params={"user_id": user_id})

    def close_browser(self, user_id: str) -> dict:
        """关闭浏览器"""
        return self._request("GET", self.endpoints["close_browser"], params={"user_id": user_id})

    def delete_browser(self, user_ids: List[str]) -> dict:
        """删除浏览器"""
        return self._request("POST", self.endpoints["delete_browser"], json={"user_ids": user_ids})

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
        if not include_all:
            # 只获取爬虫分组的浏览器
            group_id = self.get_or_create_spider_group()
            params = {"group_id": group_id}
        else:
            # 获取所有浏览器
            params = {}
            
        resp = self._request("GET", self.endpoints["get_browser_list"], params=params)
        return resp.get("data", {}).get("list", [])

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

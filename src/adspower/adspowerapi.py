import httpx
import time
import random
from typing import List, Optional, Dict, Union, Literal
import logging

logger = logging.getLogger(__name__)

class AdsPowerAPI:
    def __init__(self, base_url: str = "http://local.adspower.net:50325"):
    # def __init__(self, base_url: str = "http://127.0.0.1:50325"):
        self.base_url = base_url
        self.endpoints = {
            "start_browser": "/api/v1/browser/start",
            "close_browser": "/api/v1/browser/stop",
            "create_browser": "/api/v1/user/create",
            "get_browser_list": "/api/v1/user/list",
            "get_group_list": "/api/v1/group/list",
            "create_group": "/api/v1/group/create",
            "delete_browser": "/api/v1/user/delete"
        }

    def _request(self, method: str, endpoint: str, params: dict = None, json: dict = None, timeout: int = 60,
                 retries: int = 3) -> dict:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                response = httpx.request(method, url, params=params, json=json, timeout=timeout)
                response.raise_for_status()
                data = response.json()
                time.sleep(1)  # 保证调用频率符合限制
                return data
            except httpx.ReadTimeout:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                else:
                    raise
        return None

    def get_group_list(self) -> List[dict]:
        resp = self._request("GET", self.endpoints["get_group_list"])
        return resp.get("data", {}).get("list", [])

    def get_or_create_random_group(self) -> str:
        groups = self.get_group_list()
        if groups:
            return str(random.choice(groups)["group_id"])
        else:
            group_name = f"auto_{int(time.time())}"
            resp = self._request("POST", self.endpoints["create_group"], json={"group_name": group_name})
            return str(resp["data"]["group_id"])

    def create_browser(self, group_id: str, name: Optional[str] = None, proxy_config: Optional[dict] = None,
                       fingerprint_config: Optional[dict] = None) -> dict:
        payload = {
            "group_id": group_id,
            "name": name or f"auto_profile_{int(time.time())}",
            "user_proxy_config": proxy_config or {"proxy_soft": "no_proxy"},
            "fingerprint_config": fingerprint_config or { # https://localapi-doc-en.adspower.com/docs/Awy6Dg
                "browser_kernel_config": {"type": "chrome", "version": "131"},
                # "browser_kernel_config": {"type": "chrome", "version": "ua_auto"},
                "random_ua": {"ua_version": [], "ua_system_version": ["Windows 10"]}
            }
        }
        return self._request("POST", self.endpoints["create_browser"], json=payload)

    def start_browser(self, user_id: str) -> dict:
        if not user_id:
            raise ValueError("user_id 不可为空")
        return self._request("GET", self.endpoints["start_browser"], params={"user_id": user_id})

    def close_browser(self, user_id: str) -> dict:
        return self._request("GET", self.endpoints["close_browser"], params={"user_id": user_id})

    def delete_browser(self, user_ids: List[str]) -> dict:
        return self._request("POST", self.endpoints["delete_browser"], json={"user_ids": user_ids})

    def get_opened_user_ids(self) -> set:
        resp = self._request("GET", "/api/v1/browser/local-active")
        # logger.info(f'opened resp:{resp}')
        if resp and resp.get("code") == 0:
            return set(item["user_id"] for item in resp["data"].get("list", []))
        return set()

    def is_browser_active(self, user_id: str) -> bool:
        try:
            local_users = self.get_opened_user_ids()
            logger.info(f'local_users:{local_users}')
            return user_id in local_users
        except Exception as e:
            logger.warning(f"使用 local-active 检查失败，降级为 /active: {e}")
            resp = self._request("GET", "/api/v1/browser/active", params={"user_id": user_id})
            return resp.get("code") == 0 and resp.get("data", {}).get("status") == "Active"

    def get_browser_list(self, group_id: Optional[str] = None) -> List[dict]:
        params = {"group_id": group_id} if group_id else {}
        resp = self._request("GET", self.endpoints["get_browser_list"], params=params)
        return resp.get("data", {}).get("list", [])

import time
import logging

logging.basicConfig(level=logging.INFO)

def test_adspower():
    api = AdsPowerAPI()

    # 1. 获取或创建随机分组
    group_id = api.get_or_create_random_group()
    logging.info(f"使用分组 ID: {group_id}")

    # 2. 创建浏览器环境
    browser = api.create_browser(group_id)
    user_id = browser["data"]["id"]
    logging.info(f"创建浏览器成功 user_id = {user_id}")

    # 3. 启动浏览器
    start_result = api.start_browser(user_id)
    logging.info(f"启动浏览器成功：{start_result['data']}")

    # 可选：等待几秒，手动查看是否已启动
    time.sleep(5)

    # 4. 关闭浏览器
    close_result = api.close_browser(user_id)
    logging.info(f"关闭浏览器结果：{close_result}")

    # 5. 删除浏览器配置（可选清理）
    delete_result = api.delete_browser([user_id])
    logging.info(f"删除浏览器配置结果：{delete_result}")

if __name__ == "__main__":
    test_adspower()

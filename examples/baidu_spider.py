import json
import scrapy
from adspower.spider import AdsPowerSpider
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class BaiduSpider(AdsPowerSpider):
    name = 'baidu'
    start_urls = ['https://www.baidu.com']
    
    def start_requests(self):
        """
        启动爬虫的入口方法
        使用上下文管理器自动管理浏览器生命周期
        """
        # 使用上下文管理器获取浏览器实例
        with self.get_browser() as driver:
            try:
                # 访问起始页
                for url in self.start_urls:
                    driver.get(url)
                    
                    # 使用显式等待确保页面加载完成
                    wait = WebDriverWait(driver, 10)
                    
                    # 等待搜索框出现并输入
                    search_input = wait.until(
                        EC.presence_of_element_located((By.ID, "kw"))
                    )
                    search_input.send_keys("AdsPower")
                    
                    # 等待搜索按钮并点击
                    search_button = wait.until(
                        EC.element_to_be_clickable((By.ID, "su"))
                    )
                    search_button.click()
                    
                    # 等待搜索结果加载
                    wait.until(
                        EC.presence_of_element_located((By.CLASS_NAME, "result"))
                    )
                    
                    # 解析搜索结果
                    results = driver.find_elements(By.CLASS_NAME, "result")
                    for result in results:
                        try:
                            title = result.find_element(By.TAG_NAME, "h3").text
                            link = result.find_element(By.TAG_NAME, "a").get_attribute("href")
                            snippet = result.find_element(By.CLASS_NAME, "content-right").text
                            
                            # 检查是否被封
                            if any(keyword in snippet.lower() for keyword in [
                                "访问异常", "验证", "blocked", "forbidden"
                            ]):
                                self.logger.warning("检测到可能被封，标记当前profile")
                                self.mark_profile_blocked()
                                return
                            
                            yield {
                                "title": title,
                                "link": link,
                                "snippet": snippet
                            }
                        except Exception as e:
                            self.logger.warning(f"解析结果失败: {e}")
                            continue
                            
            except Exception as e:
                self.logger.error(f"爬取过程出错: {e}")
                # 如果是因为反爬导致的错误，标记profile
                if any(keyword in str(e).lower() for keyword in [
                    "blocked", "forbidden", "captcha", "verify"
                ]):
                    self.mark_profile_blocked() 
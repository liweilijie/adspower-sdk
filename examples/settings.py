BOT_NAME = 'adspower_example'

SPIDER_MODULES = ['examples']
NEWSPIDER_MODULE = 'examples'

# 遵循robots.txt规则
ROBOTSTXT_OBEY = True

# 配置并发请求数
CONCURRENT_REQUESTS = 1  # 因为使用AdsPower，建议设置为1

# 配置下载延迟
DOWNLOAD_DELAY = 3

# 启用Cookie
COOKIES_ENABLED = True

# 配置日志级别
LOG_LEVEL = 'INFO'

# 配置输出
FEED_FORMAT = 'json'
FEED_URI = 'output.json'

# 禁用默认的RobotsTxtMiddleware，因为我们使用浏览器
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': None,
} 
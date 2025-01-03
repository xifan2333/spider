import requests
import urllib3
import time
from typing import Optional, Dict
from utils.logger import setup_logger
from config import PROXY_CONFIG

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = setup_logger(__name__)

class ProxyPool:
    def __init__(self, api_url: str = PROXY_CONFIG['api_url']):
        """初始化代理提取器
        
        Args:
            api_url: 代理API地址
        """
        self.api_url = api_url
        self.current_proxy = None
        self.last_fetch_time = 0  # 上次获取代理的时间戳
        self.cache_duration = 60  # 缓存时间（秒）
        self.cached_formatted_proxy = None  # 缓存的格式化代理
        
        # 认证信息
        self.auth_key = PROXY_CONFIG['auth_key']
        self.password = PROXY_CONFIG['password']
        
        # 测试配置
        self.test_url = "http://httpbin.org/ip"  # 使用httpbin测试
        self.test_timeout = 5  # 超时时间5秒
    
    def _format_proxy_url(self, proxy_addr: str) -> Dict[str, str]:
        """格式化代理地址为标准格式
        
        Args:
            proxy_addr: 原始代理地址 (ip:port)
            
        Returns:
            Dict[str, str]: 包含格式化后的代理URL的字典
        """
        proxy_url = "http://%(user)s:%(password)s@%(server)s" % {
            "user": self.auth_key,
            "password": self.password,
            "server": proxy_addr,
        }
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def _test_proxy(self, proxy: str) -> bool:
        """测试代理是否可用
        
        Args:
            proxy: 代理地址 (ip:port)
            
        Returns:
            bool: 代理是否可用
        """
        try:
            proxies = self._format_proxy_url(proxy)
            
            # 使用GET请求测试
            response = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.test_timeout,
                verify=False
            )
            
            # 检查响应是否为JSON且包含origin字段
            if response.status_code == 200:
                try:
                    result = response.json()
                    return "origin" in result
                except Exception as e:
                    logger.error(f"代理测试失败 {proxy}: {str(e)}")
                    return False
            return False
                    
        except Exception as e:
            logger.error(f"代理测试失败 {proxy}: {str(e)}")
            return False
    
    def get_proxy(self) -> Optional[Dict[str, str]]:
        """获取一个可用代理，优先使用缓存的代理
        
        Returns:
            Dict[str, str]: 包含格式化后的代理URL的字典，如果获取失败返回 None
        """
        current_time = time.time()
        
        # 如果缓存的代理未过期，直接返回
        if (self.cached_formatted_proxy and 
            self.current_proxy and 
            current_time - self.last_fetch_time < self.cache_duration):
            return self.cached_formatted_proxy
            
        try:
            # 获取新代理
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            
            # 解析代理地址
            proxy = response.text.strip()
            if not proxy:
                return None
                
            # 测试代理是否可用
            if self._test_proxy(proxy):
                self.current_proxy = proxy
                self.cached_formatted_proxy = self._format_proxy_url(proxy)
                self.last_fetch_time = current_time
                logger.info(f"获取到新代理: {proxy}")
                return self.cached_formatted_proxy
            
            return None
            
        except Exception:
            logger.error("获取代理失败,直连模式")
            return None
    
    def remove_proxy(self, proxy: str):
        """标记代理不可用
        
        Args:
            proxy: 代理地址
        """
        if proxy == self.current_proxy:
            self.current_proxy = None
            self.cached_formatted_proxy = None
            self.last_fetch_time = 0
            logger.info(f"已移除代理: {proxy}")

if __name__ == "__main__":
    pool = ProxyPool()
    proxies = pool.get_proxy()
    if proxies:
        print(f"获取到代理: {proxies}") 
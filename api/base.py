from abc import ABC
from typing import Dict, Optional
import requests

from utils.logger import setup_logger
from proxies.proxy import ProxyPool
from db.manager import init_database
from fake_useragent import UserAgent





class HotelSpiderBase(ABC):
    """酒店爬虫基类"""

    def __init__(self, **kwargs):
        """初始化爬虫

        Args:
            city: 城市名称
            city_url: 城市URL标识
        """

        # 初始化日志
        self.logger = setup_logger(self.__class__.__name__.lower())

        # 初始化代理池
        self.proxy_pool = ProxyPool()

        # 初始化数据库
        self.db = init_database()

        # 初始化会话
        self.session = requests.Session()

        self.platform = kwargs.get("platform", "")  # 默认为携程
        self.current_account = None  # 当前使用的账号
        self._proxy_pool = None  # 代理池
        self._current_proxy = None  # 当前使用的代理

    def __del__(self):
        """析构函数"""
        if hasattr(self, "db"):
            self.db.close()

    def update_cookies(self):
        """更新cookies"""
        account = self.account_pool.get_account()
        if not account:
            raise Exception("没有可用的账号")

        # 更新cookies
        self.cookies = account["cookies"]
        self.current_account = account["phone"]

        # 更新请求头中的cookie
        self.headers["Cookie"] = self.account_pool.get_cookies_str(self.cookies)

    
    def update_proxy(self):
        """更新代理"""
        try:
            proxies = self.proxy_pool.get_proxy()

            self.session.proxies.update(proxies)
            self.current_proxy = proxies.get("http", "").split("@")[-1]
            self.logger.info(f"使用代理: {self.current_proxy}")

        except Exception:
            self.logger.error("不使用代理,直连模式")
            self.session.proxies = None
            self.current_proxy = None

    def update_ua(self):
        """更新User-Agent"""
        self.headers["User-Agent"] = UserAgent(platforms="mobile").random

  
    def get_hotel_list(self, page: int = 1) -> Dict:
        """获取酒店列表

        Args:
            page: 页码

        Returns:
            Dict: {
                'total': int,  # 总数
                'has_more': bool,  # 是否有下一页
                'hotels': List[Dict]  # 酒店列表
            }
        """
        pass
  
    def get_hotel_detail(self, hotel_id: str) -> Optional[Dict]:
        """获取酒店详情(可选实现)

        Args:
            hotel_id: 酒店ID

        Returns:
            Optional[Dict]: 酒店详细信息，如果不支持则返回None
        """
        pass


    def get_hotel_comments(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店评论

        Args:
            hotel_id: 酒店ID
            page: 页码

        Returns:
            Dict: 评论数据
        """
        pass


    def get_hotel_qa(self, hotel_id: str, page: int = 1) -> Optional[Dict]:
        """获取酒店问答(可选实现)

        Args:
            hotel_id: 酒店ID
            page: 页码

        Returns:
            Optional[Dict]: 问答数据，如果不支持则返回None
        """
        pass

    
    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店信息

        Args:
            hotel_info: 酒店信息

        Returns:
            bool: 是否保存成功
        """
        pass

  
    def save_comment(self, comment_info: Dict) -> bool:
        """保存评论信息

        Args:
            comment_info: 评论信息

        Returns:
            bool: 是否保存成功
        """
        pass

    def save_qa(self, qa_info: Dict) -> bool:
        """保存问答信息(可选实现)

        Args:
            qa_info: 问答信息

        Returns:
            bool: 是否保存成功
        """
        return False

    def run(self):
        """运行爬虫"""

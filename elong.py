"""艺龙酒店爬虫"""

import json
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict

from fake_useragent import UserAgent

from accounts.elong.account import ElongAccountPool
from api.base import HotelSpiderBase
from utils.logger import setup_logger
from api.decorator import request_decorator


# 创建重试装饰器


# ================ 配置部分 ================

# API配置
BASE_URL = "https://m.elong.com/tapi/v2/list"
COMMENTS_URL = "https://m.elong.com/commonpage/getCommentList"
COMMENT_INFO_URL = "https://m.elong.com/commonpage/getCommentInfo"

# 请求头配置
BASE_HEADERS = {
    "tmapi-client": "i-eh5",
    "content-type": "application/json",
    "origin": "https://m.elong.com",
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6",
    "appfrom": "13",
    "cluster": "idc",
    "deviceid": "300ef766-699f-4eb9-aec4-0a1f67035d55",
    "h-env": "",
    "h-os": "",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Microsoft Edge";v="132"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "timezone": "8",
}


# 城市配置
CITIES = [
    {"zhname": "休斯顿", "enname": "Houston", "code": 110076723},
    {"zhname": "奥斯汀", "enname": "Austin", "code": 110076547},
    {"zhname": "达拉斯", "enname": "Dallas", "code": 110076315},
    {"zhname": "圣安东尼奥", "enname": "San Antonio", "code": 110076839},
    {"zhname": "沃思堡", "enname": "Fort Worth", "code": 110076385},
    {"zhname": "埃尔帕索", "enname": "El Paso", "code": 110077028},
    {"zhname": "阿灵顿", "enname": "Arlington", "code": 110075977},
    {"zhname": "科珀斯克里斯蒂", "enname": "Corpus Christi", "code": 110076747},
    {"zhname": "拉伯克", "enname": "Lubbock", "code": 110077084},
    {"zhname": "加尔维斯顿", "enname": "Galveston", "code": 110076765},
]

# 爬虫配置
HOTEL_LIST_PAGE_SIZE = 20
COMMENT_PAGE_SIZE = 10
REQUEST_DELAY = 0.5  # 请求延迟(秒)
COMMENT_REQUEST_DELAY = 0.2  # 评论请求延迟(秒)

logger = setup_logger(__name__)


class ElongSpider(HotelSpiderBase):
    """艺龙爬虫"""

    def __init__(self, **kwargs):
        """初始化爬虫"""
        kwargs["platform"] = "elong"
        super().__init__(**kwargs)

        # 设置请求头
        self.headers = BASE_HEADERS.copy()
        self.headers["User-Agent"] = UserAgent(platforms="mobile").random
        self.account_pool = ElongAccountPool()
        self.current_city = None

    

    @request_decorator
    def get_hotel_list(self, page: int = 1) -> Dict:
        """获取酒店列表"""
        try:
            if not self.current_city:
                logger.error("未设置当前城市信息")
                return {}

            # 获取日期范围
            check_in = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            check_out = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

            # 构建请求参数
            params = {
                "city": self.current_city["code"],
                "inDate": check_in,
                "outDate": check_out,
                "pageIndex": page,
                "pageSize": HOTEL_LIST_PAGE_SIZE,
                "scriptVersion": "2.4.99",
                "filterList": "8888_1",
                "diyIsClosed": "false",
                "_timer": str(int(time.time() * 1000)),
            }

            response = self.session.get(
                BASE_URL,
                params=params,
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            return result

        except Exception as e:
            logger.error(f"获取酒店列表失败: {str(e)} \n {traceback.format_exc()}")
            return {}

    @request_decorator
    def get_hotel_comments_info(self, hotel_id: str) -> Dict:
        """获取酒店评论信息"""

        try:
            # 获取评分数据
            params = {
                "scriptVersion": "0.0.33",
                "hotelId": hotel_id,
                "can_sale_ota_category_ids": "11,6043,6020,13,6033,105,18,75,6095",
                "_timer": str(int(time.time() * 1000)),
            }

            # 发送请求并获取响应
            response = self.session.get(COMMENT_INFO_URL, params=params)
            response.raise_for_status()
            commnets_info = response.json()
            with open("comments_info.json", "w", encoding="utf-8") as f:
                json.dump(commnets_info, f, ensure_ascii=False)
            return commnets_info
        except Exception as e:
            logger.error(f"获取酒店评论信息失败: {str(e)}")
            return {}

    @request_decorator
    def get_hotel_comments(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店评论"""
        try:
            data = {
                "objectId": hotel_id,
                "pageIndex": page,
                "pageSize": COMMENT_PAGE_SIZE,
            }

            response = self.session.post(COMMENTS_URL, json=data, headers=self.headers)
            response.raise_for_status()
            result = response.json()
            return result

        except Exception as e:
            logger.error(f"获取酒店评论失败: {str(e)}")
            return {}

    def _parse_hotel_info(self, hotel: Dict) -> Dict:
        """解析酒店信息"""
        hotel_tags = []
        if hotel.get("hotelTags"):
            hotel_tags = [tag["tagName"] for tag in hotel["hotelTags"]]

        # 构建酒店信息字典
        hotel_info = {
            "酒店ID": hotel.get("hotelId", ""),
            "酒店名称": hotel.get("hotelName", ""),
            "酒店英文名称": hotel.get("hotelNameEn", ""),
            "酒店标签": ",".join(hotel_tags),
            "酒店亮点": hotel.get("commentMainTag", ""),
            "地址": hotel.get("hotelAddress", ""),
            "地理位置": hotel.get("areaName", ""),
            "酒店等级": hotel.get("starLevelDes", ""),
            "城市名称": hotel.get("cityName", ""),
            "交通信息": hotel.get("trafficInfo", ""),
        }

        return hotel_info

    def _parse_hotel_comments_info(self, comments_info_response: Dict) -> Dict:
        """解析酒店评论信息"""
        comments_info = comments_info_response.get("data", {})
        comments_info = {
            "总评分": comments_info.get("score", 0),
            "位置评分": comments_info.get("positionScore", 0),
            "设施评分": comments_info.get("facilityScore", 0),
            "服务评分": comments_info.get("serviceScore", 0),
            "卫生评分": comments_info.get("sanitationScore", 0),
            "性价比评分": comments_info.get("costScore", 0),
            "评分描述": comments_info.get("commentDes", ""),
            "总评论数": comments_info.get("commentCount", 0),
            "好评率": round(comments_info.get("goodRate", 0) * 100, 1),
            "好评数": comments_info.get("goodCount", 0),
            "差评数": comments_info.get("badCount", 0),
            "AI评论": ""  # 默认为空字符串
        }
        return comments_info
    
    def _parse_hotel_comments(self, hotel_comments_response: Dict) -> Dict:
        """解析酒店评论"""
        comments = hotel_comments_response.get("data", {}).get("commentList", [])
        return comments

    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店数据"""
        try:
            from db.models.elong import ElongHotel

            hotel_id = hotel_info.pop("id")
            hotel_info["updated_at"] = datetime.now()

            ElongHotel.update(**hotel_info).where(
                ElongHotel.hotel_id == hotel_id
            ).execute()

            if not ElongHotel.select().where(ElongHotel.hotel_id == hotel_id).exists():
                hotel_info["hotel_id"] = hotel_id
                ElongHotel.create(**hotel_info)

            logger.info(f"保存酒店数据成功: {hotel_info['name']}")
            return True

        except Exception as e:
            logger.error(f"保存酒店数据失败: {str(e)}")
            return False

    def save_comment(self, comment_info: Dict) -> bool:
        """保存评论数据"""
        try:
            from db.models.elong import ElongComment, ElongHotel

            comment_id = comment_info.pop("comment_id")
            hotel_id = comment_info.pop("hotel_id")

            hotel = ElongHotel.get_by_id(hotel_id)
            comment_info["hotel"] = hotel

            comment, created = ElongComment.get_or_create(
                comment_id=comment_id, defaults=comment_info
            )

            if not created:
                for key, value in comment_info.items():
                    setattr(comment, key, value)
                comment.save()

            logger.info(f"保存评论数据成功: {comment_id}")
            return True

        except Exception as e:
            logger.error(f"保存评论数据失败: {str(e)}")
            return False

    def process_hotel(self, hotels_list_respone: Dict):
        """处理酒店"""
        hotels = hotels_list_respone.get("data", {}).get("hotelList", [])
        if hotels:
            for hotel in hotels:
                hotel_info = self._parse_hotel_info(hotel)
                hotel_comments_info = self.get_hotel_comments_info(hotel_info["酒店ID"])
                hotel_comments_info = self._parse_hotel_comments_info(
                    hotel_comments_info
                )
                hotel_info.update(hotel_comments_info)
                comments_num = hotel_comments_info.get("总评论数", 0)
                if comments_num > 0:
                    hotel_comments = self.get_hotel_comments(
                        hotel_info["酒店ID"], page=1
                    )
                    

        # 1. 获取酒店评分信息
        # 2. 获取酒店评论信息
        # 3. 保存酒店数据
        # 4. 保存评论数据

    def process_city(self, city: Dict):
        """处理城市"""
        self.current_city = city
        first_page_hotels = self.get_hotel_list(1)
        if first_page_hotels:
            hotel_count = first_page_hotels.get("data", {}).get("hotelCount", 0)
            logger.info(f"城市: {city['zhname']}, 酒店数量: {hotel_count}")
            page_num = (hotel_count + HOTEL_LIST_PAGE_SIZE - 1) // HOTEL_LIST_PAGE_SIZE
            for page in range(1, page_num + 1):
                if page == 1:
                    hotels_list_respone = first_page_hotels
                else:
                    hotels_list_respone = self.get_hotel_list(page)

                self.process_hotel(hotels_list_respone)

    def run(self):
        """运行爬虫"""
        for city in CITIES:
            self.process_city(city)


if __name__ == "__main__":
    spider = ElongSpider()
    spider.run()

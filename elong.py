"""艺龙酒店爬虫"""

import time
import traceback
from datetime import datetime, timedelta
from typing import Dict


from accounts.elong.account import ElongAccountPool
from api.base import HotelSpiderBase
from utils.logger import setup_logger
from api.decorator import request_decorator
from db.models.elong import ElongHotel, ElongComment

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

        # 直接使用数据库字段名
        hotel_info = {
            "hotel_id": str(hotel.get("hotelId", "")),
            "name": hotel.get("hotelName", ""),
            "name_en": hotel.get("hotelNameEn", ""),
            "address": hotel.get("hotelAddress", ""),
            "location": hotel.get("areaName", ""),
            "star": hotel.get("starLevelDes", ""),
            "tags": ",".join(hotel_tags),
            "main_tag": hotel.get("commentMainTag", ""),
            "traffic_info": hotel.get("trafficInfo", ""),
            "city_name": hotel.get("cityName", ""),
        }

        return hotel_info

    def _parse_hotel_comments_info(self, comments_info_response: Dict) -> Dict:

        """解析酒店评论信息"""

        comments_info = comments_info_response.get("data", {})
        ai_summary = ""
        if "aiSummary" in comments_info:
            if comments_info["aiSummary"] and comments_info["aiSummary"].get(
                "aiSummaryContent"
            ):
                ai_summary = comments_info["aiSummary"]["aiSummaryContent"]
        # 直接使用数据库字段名
        return {
            "score": comments_info.get("score", 0),
            "score_location": comments_info.get("positionScore", 0),
            "score_facility": comments_info.get("facilityScore", 0),
            "score_service": comments_info.get("serviceScore", 0),
            "score_hygiene": comments_info.get("sanitationScore", 0),
            "score_cost": comments_info.get("costScore", 0),
            "score_desc": comments_info.get("commentDes", ""),
            "comment_count": comments_info.get("commentCount", 0),
            "good_rate": round(comments_info.get("goodRate", 0) * 100, 1),
            "good_count": comments_info.get("goodCount", 0),
            "bad_count": comments_info.get("badCount", 0),
            "ai_summary": ai_summary,
        }

    def _parse_hotel_comment(self, comment_data: Dict) -> list:
        """解析酒店评论数据"""
        try:
            user_info = comment_data.get("commentUser", {})
            comment_ext = comment_data.get("commentExt", {})
            order_info = comment_ext.get("order", {})

            # 处理图片
            image_urls = []
            if comment_data.get("images"):
                for img in comment_data["images"]:
                    image_paths = img.get("imagePaths", [])
                    for path in image_paths:
                        if path.get("specId") == 403:
                            url = path.get("url", "")
                            if url:
                                quality = url.split("/")[-2]
                                name = url.split("/")[-1]
                                hd_name = "nw_" + name
                                hd_url = url.replace(quality, "minsu_540*1500").replace(
                                    name, hd_name
                                )
                                image_urls.append(hd_url)

            # 处理评论时间
            comment_time = comment_data.get("createTime", "")
            reply = ""
            reply_time = ""
            if comment_data.get("replys"):
                first_reply = (
                    comment_data["replys"][0] if comment_data["replys"] else {}
                )
                reply = first_reply.get("content", "")
                reply_time = first_reply.get("createTime", "")
            source = comment_data.get("source", 0)
            real_source = comment_data.get("realSource", 0)
            comment_source = (
                "Expedia"
                if real_source == 63
                else "Hotels.com"
                if real_source == 64
                else "International"
                if source == 60
                else "艺龙"
            )
            # 直接使用数据库字段名
            comment = {
                "comment_id": str(comment_data.get("commentId", "")),
                "user_name": user_info.get("nickName", ""),
                "rating": comment_data.get("commentScore", 0),
                "content": comment_data.get("content", ""),
                "checkin_time": order_info.get("checkInTime", ""),
                "room_type": order_info.get("roomTypeName", ""),
                "travel_type": comment_ext.get("travelTypeDesc", ""),
                "source": comment_source,
                "images": ",".join(image_urls),
                "image_count": len(image_urls),
                "like_count": comment_data.get("usefulCount", 0),
                "reply_content": reply,
                "reply_time": reply_time,
                "comment_time": comment_time,
                "is_hidden": False,
            }

            return comment

        except Exception as e:
            logger.error(f"解析评论数据失败: {str(e)}")
            return {}

    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店数据"""
        try:
            # 数据已经使用正确的字段名，可以直接使用
            hotel_info["image_count"] = 0
            hotel_info["video_count"] = 0

            hotel = ElongHotel.get_by_id_or_none(hotel_info["hotel_id"])
            if hotel:
                return hotel.update_hotel(hotel_info)
            else:
                ElongHotel.create_hotel(hotel_info)
                return True

        except Exception as e:
            logger.error(f"保存酒店数据失败: {str(e)}")
            return False

    def save_comment(self, comment_info: Dict, hotel_id: str) -> bool:
        """保存评论数据"""
        try:
            hotel = ElongHotel.get_by_id_or_none(hotel_id)
            if not hotel:
                logger.error(f"保存评论失败: 酒店ID {hotel_id} 不存在")
                return False

            # 处理时间字段
            try:
                if comment_info["comment_time"]:
                    comment_info["comment_time"] = datetime.fromisoformat(
                        comment_info["comment_time"].replace("Z", "+00:00")
                    )
                if comment_info["reply_time"]:
                    comment_info["reply_time"] = datetime.fromisoformat(
                        comment_info["reply_time"].replace("Z", "+00:00")
                    )
            except (ValueError, AttributeError):
                pass

            # 添加酒店关联
            comment_info["hotel"] = hotel

            comment = ElongComment.get_by_id_or_none(comment_info["comment_id"])
            if comment:
                return comment.update_comment(comment_info)
            else:
                ElongComment.create_comment(comment_info)
                return True

        except Exception as e:
            logger.error(f"保存评论数据失败: {str(e)}")
            return False

    def process_city(self, city: Dict):
        """
        处理城市数据
        Args:
            city: 城市信息字典
        """
        try:
            self.current_city = city
            self.logger.info(
                f"\n{'='*20} 开始采集城市: {city['zhname']} ({city['enname']}) {'='*20}"
            )

            # 获取第一页以确定总数
            self.logger.info(f"正在获取城市 {city['zhname']} 的酒店列表...")
            first_page_hotels = self.get_hotel_list(1)
            if not first_page_hotels:
                self.logger.error(f"获取城市 {city['zhname']} 酒店列表失败")
                return

            hotel_count = first_page_hotels.get("data", {}).get("hotelCount", 0)
            if not hotel_count:
                self.logger.warning(f"城市 {city['zhname']} 没有找到酒店")
                return

            page_num = (hotel_count + HOTEL_LIST_PAGE_SIZE - 1) // HOTEL_LIST_PAGE_SIZE
            self.logger.info(
                f"城市 {city['zhname']} 共找到 {hotel_count} 家酒店，分 {page_num} 页"
            )

            # 处理所有页面
            processed_count = 0
            success_count = 0
            failed_count = 0
            total_comments = 0
            saved_comments = 0

            for page in range(1, page_num + 1):
                self.logger.info(f"\n正在获取第 {page}/{page_num} 页酒店列表...")

                # 获取当前页酒店列表
                if page == 1:
                    hotels_list_response = first_page_hotels
                else:
                    hotels_list_response = self.get_hotel_list(page)

                if not hotels_list_response:
                    self.logger.error(f"获取第 {page} 页酒店列表失败")
                    continue

                # 处理当前页的酒店
                hotels = hotels_list_response.get("data", {}).get("hotelList", [])
                if not hotels:
                    self.logger.warning(f"第 {page} 页没有酒店数据")
                    continue

                self.logger.info(f"第 {page} 页共有 {len(hotels)} 家酒店")

                for hotel in hotels:
                    try:
                        # 1. 解析酒店基本信息
                        hotel_info = self._parse_hotel_info(hotel)
                        hotel_id = hotel_info["hotel_id"]
                        self.logger.info(
                            f"\n{'='*10} 开始处理酒店: {hotel_info['name']} {'='*10}"
                        )

                        # 2. 获取并解析酒店评分信息
                        self.logger.info("正在获取酒店评分信息...")
                        hotel_comments_info = self.get_hotel_comments_info(hotel_id)
                        if hotel_comments_info:
                            comments_info = self._parse_hotel_comments_info(
                                hotel_comments_info
                            )
                            hotel_info.update(comments_info)
                            self.logger.info(
                                f"获取评分成功: 总评分 {comments_info.get('score', 0)}，评论数 {comments_info.get('comment_count', 0)}"
                            )
                        else:
                            self.logger.warning("获取酒店评分信息失败")

                        # 3. 保存酒店数据
                        self.logger.info("正在保存酒店数据...")
                        if self.save_hotel(hotel_info):
                            self.logger.info("酒店数据保存成功")
                            success_count += 1
                        else:
                            self.logger.error(f"保存酒店失败: {hotel_info['name']}")
                            failed_count += 1
                            continue

                        # 4. 处理评论数据
                        comments_num = hotel_info.get("comment_count", 0)
                        total_comments += comments_num

                        if comments_num > 0:
                            comment_pages = (
                                comments_num + COMMENT_PAGE_SIZE - 1
                            ) // COMMENT_PAGE_SIZE
                            self.logger.info(
                                f"开始获取评论: 共 {comments_num} 条，分 {comment_pages} 页"
                            )

                            hotel_saved_comments = 0
                            for comment_page in range(1, comment_pages + 1):
                                self.logger.info(
                                    f"正在获取第 {comment_page}/{comment_pages} 页评论..."
                                )

                                # 获取评论数据
                                hotel_comments_response = self.get_hotel_comments(
                                    hotel_id, page=comment_page
                                )
                                if not hotel_comments_response:
                                    self.logger.error(
                                        f"获取第 {comment_page} 页评论失败"
                                    )
                                    continue

                                # 解析评论数据
                                comments = hotel_comments_response.get("data", {}).get("comments", [])
                                page_saved = 0
                                for comment in comments:
                                    comment = self._parse_hotel_comment(comment)
                                    if comment:
                                        if self.save_comment(comment, hotel_id):
                                            hotel_saved_comments += 1
                                            page_saved += 1
                                            saved_comments += 1
                                        else:
                                            self.logger.error(
                                                f"保存评论失败: {comment.get('comment_id', '')}"
                                            )

                                

                                self.logger.info(
                                    f"第 {comment_page} 页评论处理完成: 成功保存 {page_saved}/{len(comments)} 条"
                                )

                            self.logger.info(
                                f"评论获取完成: 成功保存 {hotel_saved_comments}/{comments_num} 条"
                            )

                        processed_count += 1
                        progress = processed_count / hotel_count * 100
                        self.logger.info(
                            f"\n总进度: {processed_count}/{hotel_count} ({progress:.1f}%)"
                        )
                        self.logger.info(
                            f"统计: 成功 {success_count} 家，失败 {failed_count} 家，评论 {saved_comments}/{total_comments} 条"
                        )

                    except Exception as e:
                        self.logger.error(
                            f"❌ 处理酒店数据时出错: {str(e)} \n {traceback.format_exc()}"
                        )
                        failed_count += 1
                        continue

            self.logger.info(f"\n{'='*20} 完成城市 {city['zhname']} 采集 {'='*20}")
            self.logger.info("统计信息:")
            self.logger.info(
                "酒店: 总数 {hotel_count}，成功 {success_count}，失败 {failed_count}"
            )
            self.logger.info("评论: 总数 {total_comments}，成功保存 {saved_comments}")
            self.logger.info("{'='*50}\n")

        except Exception as e:
            self.logger.error(
                f"处理城市 {city['zhname']} 时出错: {str(e)} \n {traceback.format_exc()}"
            )

    def run(self):
        """运行爬虫"""
        try:
            total_cities = len(CITIES)
            self.logger.info(f"\n{'='*20} 开始数据采集 {'='*20}")
            self.logger.info(f"共有 {total_cities} 个城市待处理")

            for city_index, city in enumerate(CITIES, 1):
                self.logger.info(
                    f"\n{'='*15} 处理第 {city_index}/{total_cities} 个城市 {'='*15}"
                )
                self.process_city(city)

        except KeyboardInterrupt:
            self.logger.info("\n 程序被用户中断")
        except Exception as e:
            self.logger.error(f"程序执行出错: {str(e)} \n {traceback.format_exc()}")
        finally:
            self.logger.info(f"\n{'='*20} 数据采集完成 {'='*20}")


if __name__ == "__main__":
    spider = ElongSpider()
    spider.run()

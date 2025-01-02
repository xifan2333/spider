import json
import traceback
from datetime import datetime, timedelta
from http.client import RemoteDisconnected
from typing import Dict

import requests
from fake_useragent import UserAgent
from urllib3.exceptions import ProtocolError

from accounts.qunar.account import QunarAccountPool
from api.base import HotelSpiderBase
from db.models.qunar import QunarComment, QunarHotel, QunarQA
from proxies.proxy import ProxyPool
from utils.logger import setup_logger
from utils.retry.decorators import create_retry_decorator

logger = setup_logger(__name__)

# 创建重试装饰器
retry_with_proxy = create_retry_decorator(
    platform="qunar",
    max_attempts=10,
    min_wait=4,
    max_wait=10,
    multiplier=1,
    exceptions=(
        requests.exceptions.RequestException,
        json.JSONDecodeError,
        RemoteDisconnected,
        ProtocolError,
        requests.exceptions.ConnectionError,
    ),
    check_empty=True,
    reraise=False,
)


def request_decorator(func):
    """请求装饰器,用于统一处理请求前的准备工作"""

    def wrapper(self, *args, **kwargs):
        # 更新请求前的必要信息
        self._update_ua()
        self._update_cookies()
        self._update_proxy()
        return func(self, *args, **kwargs)

    return wrapper


class QunarSpider(HotelSpiderBase):
    """去哪儿网爬虫"""

    # API配置
    HOTEL_LIST_URL = "https://touch.qunar.com/hotelcn/api/hotellist"
    HOTEL_DETAIL_URL = "https://touch.qunar.com/hotelcn/api/hoteldetail"
    COMMENT_URL = "https://touch.qunar.com/hotelcn/api/commentlist"
    QA_URL = "https://touch.qunar.com/hotelcn/api/answerlist"
    SCORE_URL = "https://touch.qunar.com/hotelcn/api/subScore"
    TRAFFIC_URL = "https://touch.qunar.com/hotelcn/api/gateway"

    def __init__(self, **kwargs):
        """初始化爬虫

        Args:
            city: 城市名称，如 "深圳"
            city_url: 城市URL，如 "shenzhen"
        """
        kwargs["platform"] = "qunar"
        super().__init__(**kwargs)

        self.city = kwargs.get("city")
        self.city_url = kwargs.get("city_url")

        # 设置请求头
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json;charset=UTF-8",
            "origin": "https://touch.qunar.com",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6",
        }

        # 设置随机User-Agent
        self.headers["User-Agent"] = UserAgent(platforms="mobile").random
        self.session.headers.update(self.headers)

        # 初始化代理池
        self.proxy_pool = ProxyPool()
        self.current_proxy = None

        # 初始化账号池
        self.account_pool = QunarAccountPool()
        self.current_account = None

        # 初始化代理
        proxies = self.proxy_pool.get_proxy()
        if proxies:
            self.session.proxies = proxies
            self.current_proxy = proxies.get("http", "").split("@")[-1]
            logger.info(f"初始代理: {self.current_proxy}")
        else:
            logger.error("无法获取初始代理")

    def _update_cookies(self):
        """更新cookies"""
        account = self.account_pool.get_account()
        if not account:
            raise Exception("没有可用的账号")

        # 更新cookies
        self.cookies = account["cookies"]
        self.current_account = account["phone"]

        # 更新请求头中的cookie
        self.headers["Cookie"] = self.account_pool.get_cookies_str(self.cookies)

        # 更新 headers

        # 更新session headers
        self.session.headers.update(self.headers)
        logger.info(f"更新cookies成功: {self.current_account}")

    def _update_ua(self):
        """更新User-Agent"""
        self.headers["User-Agent"] = UserAgent(platforms="mobile").random

    @retry_with_proxy
    @request_decorator
    def get_hotel_list(self, page: int = 1) -> Dict:
        """获取酒店列表"""
        try:
            # 获取日期范围
            check_in = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            check_out = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

            payload = {
                "city": self.city,
                "cityUrl": self.city_url,
                "checkInDate": check_in,
                "checkOutDate": check_out,
                "page": page,
            }

            response = self.session.post(
                self.HOTEL_LIST_URL,
                json=payload,
            )
            response.raise_for_status()
            result = response.json()

            if result.get("ret"):
                data = result.get("data", {})
                hotels = data.get("hotels", [])
                parsed_hotels = [self._parse_hotel_item(hotel) for hotel in hotels]

                logger.info(
                    f"获取酒店列表成功: {self.city} 第 {page} 页, 共 {len(parsed_hotels)} 家酒店"
                )

                return {
                    "total": data.get("tcount", 0),
                    "has_more": data.get("hasMore", False),
                    "hotels": parsed_hotels,
                }

            return {}

        except Exception as e:
            logger.error(f"获取酒店列表失败: {str(e)},{traceback.format_exc()}")
            return {}

    def _parse_hotel_item(self, hotel: Dict) -> Dict:
        """解析单个酒店数据"""
        # 获取等级信息
        level = hotel.get("dangciText", "")
        medals = []
        if "newMedalAttrs" in hotel and hotel["newMedalAttrs"]:
            medals = [
                attr.get("title", "")
                for attr in hotel["newMedalAttrs"]
                if attr.get("title")
            ]

        # 组合等级信息：所有勋章+档次
        hotel_level = "/".join(medals + [level]) if medals else level

        # 解析经纬度
        gpoint = hotel.get("gpoint", "").split(",")
        latitude = float(gpoint[0]) if len(gpoint) > 0 else None
        longitude = float(gpoint[1]) if len(gpoint) > 1 else None

        # 获取一句话亮点
        highlight = ""
        if "labels" in hotel:
            for label in hotel["labels"]:
                if label.get("configCode") == "comment_one_word_hightpoint_label":
                    highlight = label.get("label", "")
                    break

        return {
            "name": hotel.get("name", ""),
            "id": hotel.get("seqNo", ""),
            "latitude": latitude,
            "longitude": longitude,
            "level": hotel_level,
            "score": float(hotel.get("score", 0)),
            "location": hotel.get("locationInfo", ""),
            "comment_count": int(hotel.get("commentCount", 0)),
            "highlight": highlight,
        }

    @retry_with_proxy
    @request_decorator
    def get_hotel_detail(self, hotel_id: str) -> Dict:
        """获取酒店详情"""
      
        check_in = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        check_out = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

        payload = {
            "checkInDate": check_in,
            "checkOutDate": check_out,
            "seq": hotel_id,
            "location": "",
            "cityUrl": self.city_url,
            "selectedIndex": "0",
        }

        response = self.session.post(self.HOTEL_DETAIL_URL, json=payload)
        response.raise_for_status()

        result = response.json()
        detail = self._parse_hotel_detail(result)

        # 获取评分信息
        scores = self._get_hotel_score(hotel_id)
        if scores:
            detail.update(scores)

        # 获取交通信息
        traffic = self._get_hotel_traffic(
            hotel_id,
            gpoint=detail.get("gpoint"),
            hot_poi=detail.get("location_advantage", "")
            .split("距离")[-1]
            .split("米")[0],
        )
        if traffic:
            detail["traffic"] = traffic

        self.logger.info(f"获取酒店详情成功: {hotel_id}")
        return detail

    def _parse_hotel_detail(self, detail: Dict) -> Dict:
        """解析酒店详情数据"""
        if not detail.get("ret") or "data" not in detail:
            return {}

        data = detail["data"]
        dinfo = data.get("dinfo", {})
        comment_info = data.get("commentInfo", {})

        # 解析评论标签
        comment_tags = []
        hotel_comment = dinfo.get("hotelCommentModule", {})
        if hotel_comment:
            ugc_tags = hotel_comment.get("ugcCommentTags", [])
            ugc_tags.sort(key=lambda x: int(x.get("tagCount", 0)), reverse=True)
            comment_tags = [f"{tag['tagDesc']}({tag['tagCount']})" for tag in ugc_tags]

        # 解析榜单信息
        hot_sale = data.get("hotSaleCard", {})
        ranking = (
            f"{hot_sale.get('detailDesc', '')}第{hot_sale.get('top', '')}名"
            if hot_sale
            else ""
        )

        # 解析地点优势
        location_advantage = ""
        if dinfo.get("hotPoi") and dinfo.get("hotPoiDistance"):
            location_advantage = f"距离{dinfo['hotPoi']}{dinfo['hotPoiDistance']}米"

        # 解析设施服务信息
        facilities = {}
        for service in data.get("servicePics", []):
            tag = service.get("tag", "其他")
            if tag not in facilities:
                facilities[tag] = []
            facilities[tag].append(service.get("name", ""))

        # 获取第一个电话号码
        phone = dinfo.get("phone", "")

        # 获取一句话亮点
        highlight = ""
        if "tagGroups" in data and "oneSentenceComment" in data["tagGroups"]:
            highlight = data["tagGroups"]["oneSentenceComment"].get("title", "")

        return {
            "address": dinfo.get("add", ""),
            "open_time": dinfo.get("whenOpen", ""),
            "renovation_time": dinfo.get("whenFitment", ""),
            "phone": phone,
            "comment_tags": " / ".join(comment_tags),
            "ranking": ranking,
            "good_rate": comment_info.get("goodRate", ""),
            "location_advantage": location_advantage,
            "facilities": facilities,
            "highlight": highlight,
            "gpoint": dinfo.get("gpoint", ""),
        }

    @retry_with_proxy
    @request_decorator
    def get_hotel_comments(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店评论"""

        payload = {"seq": hotel_id, "page": page}

        response = self.session.post(self.COMMENT_URL, json=payload)
        response.raise_for_status()

        result = response.json()
        if result.get("ret"):
            data = result.get("data", {})
            comments = data.get("list", [])
            parsed_comments = [
                self._parse_comment(comment, hotel_id) for comment in comments
            ]

            # 更新AI总结（仅在第一页时）
            if page == 1 and "aiSummary" in data:
                ai_summary = data["aiSummary"].get("text", {}).get("content", "")
                if ai_summary:
                    QunarHotel.update(
                        ai_summary=ai_summary, updated_at=datetime.now()
                    ).where(QunarHotel.hotel_id == hotel_id).execute()

            total_count = data.get("count", 0)
            page_size = data.get("pageSize", 15)
            total_pages = (total_count + page_size - 1) // page_size

            self.logger.info(f"获取酒店评论成功: {hotel_id} 第 {page}/{total_pages} 页")

            return {
                "comments": parsed_comments,
                "total_pages": total_pages,
                "has_more": page * page_size < total_count,
            }

        return {}

    def _parse_comment(self, comment: Dict, hotel_id: str) -> Dict:
        """解析评论数据"""
        content_data = comment.get("contentData", {})

        # 处理评论图片
        image_urls = []
        image_infos = content_data.get("imageInfos", [])
        if image_infos:
            for img in image_infos:
                if img.get("url"):
                    image_url = f"https://ugcimg.qunarzz.com/imgs/{img['url']}i640.jpg"
                    image_urls.append(image_url)

        # 处理酒店回复
        reply_content = ""
        reply_time = ""
        replies = comment.get("reply", [])
        if replies and len(replies) > 0:
            first_reply = replies[0]
            reply_content = first_reply.get("content", "")
            reply_time = first_reply.get("time", "")

        # 解析评论时间
        feed_time = None
        if comment.get("feedTime"):
            feed_time = datetime.fromtimestamp(comment["feedTime"] / 1000)

        return {
            "comment_id": str(comment.get("feedOid", "")),
            "hotel_id": hotel_id,
            "user_name": comment.get("nickName", ""),
            "rating": float(content_data.get("evaluation", 0)),
            "content": content_data.get("feedContent", ""),
            "checkin_time": content_data.get("checkInDate", ""),
            "room_type": content_data.get("roomType", ""),
            "travel_type": content_data.get("tripType", ""),
            "source": content_data.get("from", ""),
            "ip_location": comment.get("ipLocation", ""),
            "images": ",".join(image_urls) if image_urls else None,
            "image_count": len(image_urls),
            "like_count": content_data.get("stat", {}).get("likeCount", 0),
            "reply_content": reply_content,
            "reply_time": reply_time,
            "feed_time": feed_time,
        }

    @retry_with_proxy
    @request_decorator
    def get_hotel_qa(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店问答"""
        payload = {"seq": hotel_id, "page": page, "pageSize": 15}

        payload = {"seq": hotel_id, "page": page, "pageSize": 15}

        response = self.session.post(self.QA_URL, json=payload)
        response.raise_for_status()

        result = response.json()
        if result.get("ret"):
            data = result.get("data", {})
            qa_list = data.get("content", [])
            parsed_qa = [self._parse_qa(qa, hotel_id) for qa in qa_list]

            total_count = data.get("totalRows", 0)
            page_size = 15
            total_pages = (total_count + page_size - 1) // page_size

            self.logger.info(f"获取酒店问答成功: {hotel_id} 第 {page}/{total_pages} 页")

            return {
                "qa_list": parsed_qa,
                "total_pages": total_pages,
                "has_more": page * page_size < total_count,
            }

        return {}

    def _parse_qa(self, qa: Dict, hotel_id: str) -> Dict:
        """解析问答数据"""
        # 处理回答列表
        answer_list = qa.get("answerList", [])
        formatted_replies = []

        for idx, answer in enumerate(answer_list, 1):
            reply_content = answer.get("content", "")
            if reply_content:
                formatted_replies.append(f"{idx}. {reply_content}")

        return {
            "qa_id": str(qa.get("id", "")),
            "hotel_id": hotel_id,
            "question": qa.get("title", ""),
            "ask_time": datetime.strptime(qa.get("createTime", ""), "%Y-%m-%d"),
            "asker": qa.get("ext1", {}).get("nick", "去哪儿用户"),
            "reply_count": len(answer_list),
            "replies": " ".join(formatted_replies) if formatted_replies else None,
        }

    def _get_hotel_score(self, hotel_id: str) -> Dict:
        """获取酒店评分"""
        try:
            response = self.session.post(self.SCORE_URL, json={"seq": hotel_id})
            response.raise_for_status()

            result = response.json()
            if result.get("ret") and "data" in result:
                scores = {}
                for item in result["data"]:
                    scores[item["name"]] = item["score"]

                return {
                    "score_service": float(scores.get("服务", 0)),
                    "score_location": float(scores.get("位置", 0)),
                    "score_facility": float(scores.get("设施", 0)),
                    "score_hygiene": float(scores.get("卫生", 0)),
                }

        except Exception as e:
            self.logger.error(f"获取酒店评分失败: {str(e)}")

        return {}

    def _get_hotel_traffic(
        self, hotel_id: str, gpoint: str = None, hot_poi: str = None
    ) -> Dict:
        """获取酒店交通信息"""
        try:
            b_params = {
                "hotelSeq": hotel_id,
                "cityUrl": self.city_url,
                "gpoint": gpoint or "",
                "hotPoi": hot_poi or "",
                "coordConvert": 1,
                "bizVersion": 17,
            }

            c_params = {
                "h_ct": "TCH",
                "adid": "",
                "brush": "",
                "cas": "",
                "catom": "",
                "cid": "s%3Dgoogle",
                "gid": "e0d7376a-92d5-4b0f-802e-107a33f57336",
                "ke": "",
                "lat": "",
                "lgt": "",
                "ma": "",
                "mno": "",
                "model": "XT910",
                "msg": "",
                "nt": "",
                "osVersion": "4.0.4_15",
                "pid": 10060,
                "ref": "",
                "sid": "",
                "uid": "e0d7376a-92d5-4b0f-802e-107a33f57336",
                "un": "woqjsci8835",
                "vid": 91010000,
            }

            payload = {
                "b": json.dumps(b_params),
                "c": json.dumps(c_params),
                "qrt": "h_hhotdog_trafficAround",
            }

            response = self.session.post(self.TRAFFIC_URL, data=json.dumps(payload))
            response.raise_for_status()

            result = response.json()
            if result.get("ret") and "data" in result:
                data = result["data"]["data"]["trafficAround"]["trafficModels"]

                traffic_info = {
                    "subway": [],
                    "airport": [],
                    "railway": [],
                    "bus": [],
                    "landmark": [],
                }

                type_mapping = {
                    "飞机场": "airport",
                    "火车站": "railway",
                    "地铁站": "subway",
                    "汽车站": "bus",
                    "地标": "landmark",
                }

                for item in data:
                    item_type = type_mapping.get(item.get("name", ""))
                    if item_type and item_type in traffic_info:
                        for info in item.get("infos", []):
                            traffic_info[item_type].append(
                                {
                                    "name": info.get("addr", ""),
                                    "distance": info.get("distanceStr", ""),
                                }
                            )

                return traffic_info

        except Exception as e:
            self.logger.error(f"获取酒店交通信息失败: {str(e)}")

        return {}

    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店数据"""
        try:
            # 基本信息
            data = {
                "hotel_id": hotel_info["id"],
                "name": hotel_info["name"],
                "level": hotel_info["level"],
                "location": hotel_info["location"],
                "longitude": str(hotel_info["longitude"]),
                "latitude": str(hotel_info["latitude"]),
                "score": hotel_info["score"],
                "comment_count": hotel_info["comment_count"],
                "highlight": hotel_info["highlight"],
                "updated_at": datetime.now(),
            }

            # 详情信息
            if "address" in hotel_info:
                data.update(
                    {
                        "address": hotel_info["address"],
                        "open_time": hotel_info["open_time"],
                        "renovation_time": hotel_info["renovation_time"],
                        "phone": hotel_info["phone"],
                        "comment_tags": hotel_info["comment_tags"],
                        "ranking": hotel_info["ranking"],
                        "good_rate": hotel_info["good_rate"],
                        "location_advantage": hotel_info["location_advantage"],
                    }
                )

            # 设施信息
            if "facilities" in hotel_info:
                facility_list = []
                for category, items in hotel_info["facilities"].items():
                    if items:
                        facility_list.append(f"{category}：{' / '.join(items)}")
                data["facilities"] = (
                    " | ".join(facility_list) if facility_list else None
                )

            # 评分信息
            if "score_service" in hotel_info:
                data.update(
                    {
                        "score_service": hotel_info["score_service"],
                        "score_location": hotel_info["score_location"],
                        "score_facility": hotel_info["score_facility"],
                        "score_hygiene": hotel_info["score_hygiene"],
                    }
                )

            # 交通信息
            if "traffic" in hotel_info:
                traffic_list = []
                type_names = {
                    "subway": "地铁",
                    "airport": "机场",
                    "railway": "火车站",
                    "bus": "汽车站",
                    "landmark": "地标",
                }

                for traffic_type, type_name in type_names.items():
                    items = hotel_info["traffic"].get(traffic_type, [])
                    if items:
                        info_list = [
                            f"{item['name']}({item['distance']})" for item in items
                        ]
                        traffic_list.append(f"{type_name}：{' / '.join(info_list)}")

                data["traffic"] = " | ".join(traffic_list) if traffic_list else None

            # 使用update_or_create而不是replace
            hotel_id = data.pop("hotel_id")  # 从数据中移除hotel_id
            QunarHotel.update(**data).where(QunarHotel.hotel_id == hotel_id).execute()

            # 如果酒店不存在，则创建
            if not QunarHotel.select().where(QunarHotel.hotel_id == hotel_id).exists():
                data["hotel_id"] = hotel_id  # 添加回hotel_id
                QunarHotel.create(**data)

            self.logger.info(f"保存酒店数据成功: {hotel_info['name']}")
            return True

        except Exception as e:
            self.logger.error(f"保存酒店数据失败: {str(e)}")
            return False

    def save_comment(self, comment_info: Dict) -> bool:
        """保存评论数据"""
        try:
            comment_id = comment_info.pop("comment_id")
            hotel_id = comment_info.pop("hotel_id")

            # 获取酒店实例
            hotel = QunarHotel.get_by_id(hotel_id)
            comment_info["hotel"] = hotel

            # 更新或创建评论
            comment, created = QunarComment.get_or_create(
                comment_id=comment_id, defaults=comment_info
            )

            if not created:
                # 如果评论已存在，更新数据
                for key, value in comment_info.items():
                    setattr(comment, key, value)
                comment.save()

            self.logger.info(f"保存评论数据成功: {comment_id}")
            return True

        except Exception as e:
            self.logger.error(f"保存评论数据失败: {str(e)}")
            return False

    def save_qa(self, qa_info: Dict) -> bool:
        """保存问答数据"""
        try:
            qa_id = qa_info.pop("qa_id")
            hotel_id = qa_info.pop("hotel_id")

            # 获取酒店实例
            hotel = QunarHotel.get_by_id(hotel_id)
            qa_info["hotel"] = hotel

            # 更新或创建问答
            qa, created = QunarQA.get_or_create(qa_id=qa_id, defaults=qa_info)

            if not created:
                # 如果问答已存在，更新数据
                for key, value in qa_info.items():
                    setattr(qa, key, value)
                qa.save()

            self.logger.info(f"保存问答数据成功: {qa_id}")
            return True

        except Exception as e:
            self.logger.error(f"保存问答数据失败: {str(e)}")
            return False

    def _process_comments(self, hotel_id: str):
        """处理酒店评论数据"""
        try:
            current_page = 1

            while True:
                # 获取当前页评论
                result = self.get_hotel_comments(hotel_id, current_page)
                if not result:
                    break

                # 保存评论
                comments = result["comments"]
                for comment in comments:
                    self.save_comment(comment)

                self.logger.info(
                    f"处理酒店评论完成: {hotel_id}, 第 {current_page}/{result['total_pages']} 页"
                )

                # 检查是否还有下一页
                if not result["has_more"]:
                    break

                current_page += 1

        except Exception as e:
            self.logger.error(f"处理酒店评论出错: {str(e)}")
            return False

        return True

    def _process_qa(self, hotel_id: str):
        """处理酒店问答数据"""
        try:
            current_page = 1

            while True:
                # 获取当前页问答
                result = self.get_hotel_qa(hotel_id, current_page)
                if not result:
                    break

                # 保存问答
                qa_list = result["qa_list"]
                for qa in qa_list:
                    self.save_qa(qa)

                self.logger.info(
                    f"处理酒店问答完成: {hotel_id}, 第 {current_page}/{result['total_pages']} 页"
                )

                # 检查是否还有下一页
                if not result["has_more"]:
                    break

                current_page += 1

        except Exception as e:
            self.logger.error(f"处理酒店问答出错: {str(e)}")
            return False

        return True

    def _update_proxy(self):
        """更新代理"""
        try:
            proxies = self.proxy_pool.get_proxy()
            if proxies and isinstance(proxies, dict):
                self.session.proxies = proxies
                self.current_proxy = proxies.get("http", "").split("@")[-1]
                logger.info(f"更新代理成功: {self.current_proxy}")
            else:
                # 如果获取不到代理，清空当前代理设置
                self.session.proxies = {}
                self.current_proxy = None
                logger.warning("无法获取代理，已清空代理设置")
        except Exception as e:
            logger.error(f"更新代理失败: {str(e)}")
            # 出错时也清空代理设置
            self.session.proxies = {}
            self.current_proxy = None


def main():
    """主函数"""
    # 初始化爬虫
    crawler = QunarSpider(city="深圳", city_url="shenzhen")

    # 运行采集
    crawler.run()


if __name__ == "__main__":
    main()

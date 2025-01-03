import json
import traceback
from datetime import datetime, timedelta
from typing import Dict, List

from accounts.qunar.account import QunarAccountPool
from api.base import HotelSpiderBase
from db.models.qunar import QunarComment, QunarHotel, QunarQA
from proxies.proxy import ProxyPool
from utils.logger import setup_logger
from api.decorator import request_decorator

logger = setup_logger(__name__)

HOTEL_LIST_PAGE_SIZE = 20
COMMENTS_PAGE_SIZE = 15
QA_PAGE_SIZE = 15


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
            "Content-Type": "application/json; charset=UTF-8",
            "content-type": "application/json; charset=UTF-8",
            "origin": "https://touch.qunar.com",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6",
        }

        # 设置随机User-Agent
        self.session.headers.update(self.headers)

        # 初始化代理池
        self.proxy_pool = ProxyPool()
        self.current_proxy = None

        # 初始化账号池
        self.account_pool = QunarAccountPool()
        self.current_account = None

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

            return result

        except Exception as e:
            logger.error(f"获取酒店列表失败: {str(e)},{traceback.format_exc()}")
            return {}

    @request_decorator
    def get_hotel_detail(self, hotel_id: str) -> Dict:
        """获取酒店详情"""
        try:
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
            return result
        except Exception as e:
            logger.error(f"获取酒店详情失败: {str(e)},{traceback.format_exc()}")
            return {}

    @request_decorator
    def get_hotel_comments(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店评论"""
        try:
            payload = {"seq": hotel_id, "page": page}

            response = self.session.post(self.COMMENT_URL, json=payload)
            response.raise_for_status()

            result = response.json()
            return result
        except Exception as e:
            logger.error(f"获取酒店评论失败: {str(e)},{traceback.format_exc()}")
            return {}

    @request_decorator
    def get_hotel_qas(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店问答"""
        try:
            payload = {"seq": hotel_id, "page": page, "pageSize": 15}

            response = self.session.post(self.QA_URL, json=payload)
            response.raise_for_status()

            result = response.json()
            return result
        except Exception as e:
            logger.error(f"获取酒店问答失败: {str(e)},{traceback.format_exc()}")
            return {}

    @request_decorator
    def get_hotel_score(self, hotel_id: str) -> Dict:
        """获取酒店评分"""
        try:
            response = self.session.post(self.SCORE_URL, json={"seq": hotel_id})
            response.raise_for_status()

            result = response.json()
            return result

        except Exception as e:
            self.logger.error(f"获取酒店评分失败: {str(e)}")

        return {}

    @request_decorator
    def get_hotel_traffic(
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
            return result
        except Exception as e:
            self.logger.error(f"获取酒店交通信息失败: {str(e)}")

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
                if label.get("description") == "评论数后一句话标签":
                    highlight = label.get("label", "")
                    break

        return {
            "酒店名称": hotel.get("name", ""),
            "酒店ID": hotel.get("seqNo", ""),
            "纬度": latitude,
            "经度": longitude,
            "等级": hotel_level,
            "评分": float(hotel.get("score", 0)),
            "地址": hotel.get("locationInfo", ""),
            "评论数": int(hotel.get("commentCount", 0)),
            "一句话亮点": highlight,
        }

    def _parse_hotel_detail(self, detail: Dict) -> Dict:
        """解析酒店详情数据"""
        if not detail.get("ret") or "data" not in detail:
            return {}

        data = detail["data"]
        dinfo = data.get("dinfo", {})
        comment_info = data.get("commentInfo", {})
        new_medal_attrs = data.get("newMedalAttrs", [])

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
        facility_texts = []
        for service in data.get("servicePics", []):
            tag = service.get("tag", "其他")
            name = service.get("name", "")
            if tag and name:
                facility_texts.append(f"{tag}：{name}")

        # 获取第一个电话号码
        phone = dinfo.get("phone", "")

        is_platform_choice = False
        if new_medal_attrs:
            for medal in new_medal_attrs:
                if (
                    medal.get("imgUrl")
                    == "https://s.qunarzz.com/f_cms/2022/1650508766856_113033638.png"
                ):
                    is_platform_choice = True
                    break

        return {
            "en_name": dinfo.get("enName", ""),
            "address": dinfo.get("add", ""),
            "open_time": dinfo.get("whenOpen", ""),
            "fitment_time": dinfo.get("whenFitment", ""),
            "room_count": dinfo.get("rnum", ""),
            "phone": phone,
            "comment_tags": " / ".join(comment_tags),
            "ranking": ranking,
            "good_rate": comment_info.get("goodRate", ""),
            "location_advantage": location_advantage,
            "facilities": " | ".join(facility_texts),
            "is_platform_choice": is_platform_choice,
        }

    def _parse_hotel_traffic(self, traffic: Dict) -> Dict:
        """解析酒店交通信息"""
        try:
            if not traffic.get("ret"):
                return {}

            traffic_data = (
                traffic.get("data", {})
                .get("data", {})
                .get("trafficAround", {})
                .get("trafficModels", [])
            )
            traffic_texts = []

            for model in traffic_data:
                name = model.get("name", "")
                infos = model.get("infos", [])
                if infos:
                    info_texts = []
                    for info in infos:
                        addr = info.get("addr", "")
                        distance = info.get("distanceStr", "")
                        if addr and distance:
                            info_texts.append(f"{addr} {distance}")
                    if info_texts:
                        traffic_texts.append(f"{name}：{' / '.join(info_texts)}")

            return {"traffic_info": " | ".join(traffic_texts)}
        except Exception as e:
            self.logger.error(f"解析酒店交通信息失败: {str(e)}")
            return {"traffic_info": ""}

    def _parse_hotel_score(self, score: Dict) -> Dict:
        """解析酒店评分"""
        try:
            score_data = score.get("data", [])
            score_texts = []
            for item in score_data:
                score_texts.append(f"{item['name']}:{item['score']}")
            return {"detail_score": "/".join(score_texts)}
        except Exception as e:
            self.logger.error(f"解析酒店评分失败: {str(e)}")
            return {"detail_score": ""}

    def parse_comment(self, comment: Dict, hotel_id: str) -> Dict:
        """解析评论数据"""
        feed_id = comment.get("feedOid", "")
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
            feed_time = datetime.fromtimestamp(comment["feedTime"] / 1000).strftime("%Y-%m-%d %H:%M:%S")

        return {
            "评论ID": feed_id,
            "酒店ID": hotel_id,
            "用户名": comment.get("nickName", ""),
            "评分": float(content_data.get("evaluation", 0)),
            "内容": content_data.get("feedContent", ""),
            "入住时间": content_data.get("checkInDate", ""),
            "房型": content_data.get("roomType", ""),
            "出行类型": content_data.get("tripType", ""),
            "来源": content_data.get("from", ""),
            "IP地址": comment.get("ipLocation", ""),
            "图片": ",".join(image_urls) if image_urls else None,
            "图片数量": len(image_urls),
            "点赞数": content_data.get("stat", {}).get("likeCount", 0),
            "回复内容": reply_content,
            "回复时间": reply_time,
            "评论时间": feed_time,
        }

    def parse_qa(self, qa: Dict, hotel_id: str) -> List[Dict]:
        """解析问答数据"""
        qa_records = []

        # 基础问题信息
        base_qa = {
            "qa_id": str(qa.get("id", "")),
            "hotel": hotel_id,
            "question": qa.get("title", ""),
            "asker_nickname": qa.get("ext1", {}).get("nick", ""),
            "ask_time": qa.get("createTime", ""),
            "answer_count": qa.get("answerCount", 0),
            "question_source": qa.get("faqSourceText", ""),
        }

        # 处理回答列表
        answer_list = qa.get("answerList", [])
        if not answer_list:
            # 如果没有回答，仍然保存问题记录
            qa_records.append(
                {
                    **base_qa,
                    "answer_id": "",
                    "answerer_nickname": "",
                    "answer_time": "",
                    "answer_content": "",
                    "is_official": False,
                }
            )
        else:
            # 每个回答生成一条记录
            for answer in answer_list:
                qa_record = {
                    **base_qa,
                    "answer_id": str(answer.get("id", "")),
                    "answerer_nickname": answer.get("userNick", ""),
                    "answer_time": answer.get("createTime", ""),
                    "answer_content": answer.get("content", ""),
                    "is_official": answer.get("isOfficialAnswer", False),
                }
                qa_records.append(qa_record)

        return qa_records

    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店数据"""
        pass

    def save_comment(self, comment_info: Dict) -> bool:
        """保存评论数据"""
        pass

    def save_qa(self, qa_info: Dict) -> bool:
        """保存问答数据"""
        pass

    def process_hotels(self, hotel_list_response: Dict):
        """处理酒店列表数据"""
        hotel_list = hotel_list_response.get("data", {}).get("hotels", [])
        total_hotels = len(hotel_list)
        
       
        
        for index, hotel in enumerate(hotel_list, 1):
            hotel_id = hotel["seqNo"]
            logger.info(f"正在处理第{index}/{total_hotels}家酒店: {hotel_id}")
            
            try:
                # 1. 处理酒店基础信息
                base_info = self._parse_hotel_item(hotel)
                logger.debug(f"获取酒店基础信息成功: {hotel_id}")
                
                # 2. 处理酒店详情
                detail_info = self.get_hotel_detail(hotel_id)
                detail_info = self._parse_hotel_detail(detail_info)
                logger.debug(f"获取酒店详情成功: {hotel_id}")
                
                # 3. 处理评分信息
                score_info = self.get_hotel_score(hotel_id)
                score_info = self._parse_hotel_score(score_info)
                logger.debug(f"获取酒店评分成功: {hotel_id}")
                
                # 4. 处理交通信息
                traffic_info = self.get_hotel_traffic(hotel_id)
                traffic_info = self._parse_hotel_traffic(traffic_info)
                logger.debug(f"获取酒店交通信息成功: {hotel_id}")

                # 合并所有酒店信息
                info = {**base_info, **detail_info, **score_info, **traffic_info}

                # 5. 处理AI点评
                first_page_comments = self.get_hotel_comments(hotel_id, 1)
                comments_counts = first_page_comments.get("data", {}).get("count", 0)

                try:
                    aiSummary = first_page_comments.get("data", {}).get("aiSummary", {})
                    aiSummary_text = aiSummary.get("text", {}).get("content", "")
                    aiSummary_album = aiSummary.get("album", {}).get("imageCovers", [])
                    aiSummary_image_urls = ",".join(
                        [image.get("url", "") for image in aiSummary_album]
                    )
                    logger.debug(f"解析AI点评成功: {hotel_id}")
                except Exception as e:
                    logger.error(f"解析AI点评失败: {hotel_id}, {str(e)}")
                    aiSummary_text = ""
                    aiSummary_image_urls = ""

                info["AI点评"] = aiSummary_text if aiSummary_text else ""
                info["AI点评图片"] = aiSummary_image_urls if aiSummary_image_urls else ""

                

                

                # 6. 保存酒店信息
                hotel_model = QunarHotel.get_by_id_or_none(hotel_id)
                if hotel_model:
                    hotel_model.update_hotel(info)
                    logger.info(f"更新酒店信息成功: {hotel_id}")
                else:
                    QunarHotel.create_hotel(info)
                    logger.info(f"创建酒店信息成功: {hotel_id}")

                # 7. 处理评论数据
                # total_pages = (comments_counts + COMMENTS_PAGE_SIZE - 1) // COMMENTS_PAGE_SIZE
                # logger.info(f"开始处理酒店评论, 共{total_pages}页: {hotel_id}")

                # for page in range(1, total_pages + 1):
                #     logger.debug(f"正在处理第{page}/{total_pages}页评论: {hotel_id}")
                    
                #     if page == 1:
                #         comments = first_page_comments
                #     else:
                #         comments = self.get_hotel_comments(hotel_id, page)

                #     comments_list = comments.get("data", {}).get("list", [])
                #     for comment_data in comments_list:
                #         try:
                #             comment_info = self.parse_comment(comment_data, hotel_id)
                #             comment_id = comment_info["评论ID"]
                            
                            
                #             comment_model = QunarComment.get_by_id_or_none(comment_id)
                #             if comment_model:
                #                 comment_model.update_comment(comment_info)
                #                 logger.debug(f"更新评论成功: {comment_id}")
                #             else:
                #                 QunarComment.create_comment(comment_info)
                #                 logger.debug(f"创建评论成功: {comment_id}")
                #         except Exception as e:
                #             logger.error(f"处理评论失败: {str(e)}")

                # 8. 处理问答数据
                first_page_qas = self.get_hotel_qas(hotel_id, 1)
                qas_counts = first_page_qas.get("data", {}).get("totalRows", 0)
                total_pages = (qas_counts + QA_PAGE_SIZE - 1) // QA_PAGE_SIZE
                
                logger.info(f"开始处理酒店问答, 共{total_pages}页: {hotel_id}")

                for page in range(1, total_pages + 1):
                    logger.debug(f"正在处理第{page}/{total_pages}页问答: {hotel_id}")
                    
                    if page == 1:
                        qas = first_page_qas
                    else:
                        qas = self.get_hotel_qas(hotel_id, page)
                        
                    if qas.get("data", {}).get("content", []):
                        content = qas.get("data", {}).get("content", [])
                        for qa_item in content:
                            try:
                                qa_records = self.parse_qa(qa_item, hotel_id)
                                for qa_info in qa_records:
                                    qa_id = qa_info["qa_id"]
                                    
                                    qa_model = QunarQA.get_by_id_or_none(qa_id)
                                    if qa_model:
                                        qa_model.update_qa(qa_info)
                                        logger.debug(f"更新问答成功: {qa_id}")
                                    else:
                                        QunarQA.create_qa(qa_info)
                                        logger.debug(f"创建问答成功: {qa_id}")
                            except Exception as e:
                                logger.error(f"处理问答失败: {str(e)}")
                    
                logger.info(f"完成处理第{index}/{total_hotels}家酒店: {hotel_id}")
                
            except Exception as e:
                logger.error(f"处理酒店数据失败: {hotel_id}, {str(e)}")
                continue

        logger.info(f"完成处理所有{total_hotels}家酒店数据")

    def run(self):
        first_page = self.get_hotel_list(1)
        total_count = first_page.get("data", {}).get("tcount", 0)
        total_pages = (total_count + HOTEL_LIST_PAGE_SIZE - 1) // HOTEL_LIST_PAGE_SIZE
        logger.info(f"开始处理酒店列表, 共{total_pages}页")
        for page in range(1, total_pages + 1):
            if page == 1:
                hotel_list_response = first_page
            else:
                hotel_list_response = self.get_hotel_list(page)
            logger.info(f"正在处理第{page}/{total_pages}页酒店列表")
            self.process_hotels(hotel_list_response)


def main():
    """主函数"""
    # 初始化爬虫
    crawler = QunarSpider(city="深圳", city_url="shenzhen")

    # 运行采集
    crawler.run()


if __name__ == "__main__":
    main()

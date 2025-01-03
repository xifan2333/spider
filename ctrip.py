from typing import Dict, List
from datetime import datetime, timedelta
import json
from bs4 import BeautifulSoup
import traceback
from api.base import HotelSpiderBase
from utils.ai import AIGenerator
from db.models.ctrip import CtripHotel, CtripComment, CtripQA
from accounts.ctrip.account import CtripAccountPool
from proxies.proxy import ProxyPool
from utils.logger import setup_logger
from api.decorator import request_decorator


# ================ 配置部分 ================

# 携程API配置
CTRIP_BASE_URL = "https://m.ctrip.com/restapi/soa2/31454/gethotellist"
CTRIP_COMMENT_URL = "https://m.ctrip.com/restapi/soa2/24626/commentlist"
CTRIP_QA_URL = "https://m.ctrip.com/webapp/you/askAnswer/ask/askList"

# 请求头配置
HEADERS = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9",
    "content-type": "application/json",
    "locale": "zh-CN",
    "origin": "https://m.ctrip.com",
    "priority": "u=1, i",
}

logger = setup_logger(__name__)



class CtripSpider(HotelSpiderBase):
    """携程酒店爬虫"""

    def __init__(self, **kwargs):
        """初始化爬虫

        Args:
            province_id: 省份ID
            city_id: 城市ID
            country_id: 国家ID
        """
        kwargs["platform"] = "ctrip"
        super().__init__(**kwargs)

        # 设置请求头
        self.province_id = kwargs.get("province_id")
        self.city_id = kwargs.get("city_id")
        self.country_id = kwargs.get("country_id")

        self.headers = HEADERS.copy()

        # 初始化账号池
        self.account_pool = CtripAccountPool()

        # 初始化AI生成器
        self.ai_generator = AIGenerator()

        # 初始化代理池
        self.proxy_pool = ProxyPool()
        self.current_proxy = None

        # 统计变量
        self.total_comments = 0
        self.saved_comments = 0

    
    @request_decorator
    def get_hotel_list(self, page: int = 1) -> Dict:
        """获取酒店列表"""
        try:
            check_in = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            check_out = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

            payload = {
                "paging": {"pageIndex": page, "pageSize": 10},
                "location": {
                    "countryId": self.country_id,
                    "provinceId": self.province_id,
                    "districtId": 0,
                    "cityId": self.city_id,
                    "isOversea": True,
                },
                "date": {"checkInDate": check_in, "checkOutDate": check_out},
            }

            response = self.session.post(
                CTRIP_BASE_URL,
                json=payload,
                headers=self.headers,
                timeout=30,
                verify=False
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"获取第 {page} 页酒店列表成功")
            return result

        except Exception as e:
            logger.error(f"获取酒店列表失败: {str(e)}\n{traceback.format_exc()}")
            return {}

   
    @request_decorator
    def get_hotel_comments(self, hotel_id: str, page: int = 1) -> Dict:
        """获取酒店评论"""
        try:
            
            
            payload = {
                "hotelId": hotel_id,
                "pageIndex": page,
                "pageSize": 10,
                "repeatComment": 1,
            }

            response = self.session.post(
                CTRIP_COMMENT_URL,
                json=payload,
                headers=self.headers,
                timeout=30,
                verify=False
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"获取酒店评论成功: {hotel_id} 第 {page} 页")
            return result

        except Exception as e:
            logger.error(f"获取评论失败: {str(e)}\n{traceback.format_exc()}")
            return {}

  
    @request_decorator
    def get_hotel_qa(self, hotel_id: str) -> Dict:
        """获取酒店问答数据

        Args:
            hotel_id: 酒店ID

        Returns:
            Dict: 原始问答数据
        """
        try:
            params = {
                "id": hotel_id,
                "pageType": "hotel",
                "isHideNavBar": "YES",
                "isHideHeader": "true",
            }

            response = self.session.get(
                CTRIP_QA_URL, 
                params=params, 
                headers=self.headers,
                timeout=30,
                verify=False
            )
            response.raise_for_status()

            # 解析HTML获取数据
            soup = BeautifulSoup(response.text, "html.parser")
            script_tag = soup.find("script", {"id": "__NEXT_DATA__"})

            if script_tag:
                json_data = json.loads(script_tag.string)
                logger.info(f"{hotel_id} 获取问答数据成功")
                return json_data

            return {}

        except Exception as e:
            logger.error(f"获取问答数据失败: {str(e)}\n{traceback.format_exc()}")
            return {}

    def _parse_qa(self, qa_data: Dict) -> List[Dict]:
        """解析问答数据

        Args:
            qa_data: 原始问答数据

        Returns:
            List[Dict]: 解析后的问答列表
        """
        try:
            qa_list = []

            list_data = (
                qa_data.get("props", {})
                .get("pageProps", {})
                .get("initialState", {})
                .get("listData", [])
            )
            
            for item in list_data:
                if not item:  # 跳过空数据
                    continue
                    
                # 处理回复列表
                reply_list = item.get("replyList") or []
                formatted_replies = []

                for idx, reply in enumerate(reply_list, 1):
                    if not reply:
                        continue
                    reply_content = self._clean_text(reply.get("content", ""))
                    nickname = reply.get("nickName", "热心网友")
                    if reply_content:
                        formatted_replies.append(f"{idx}. {nickname}：{reply_content}")

                # 解析提问时间
                ask_time = None
                create_time = item.get("createTime", "")
                if create_time:
                    try:
                        ask_time = self._parse_date(create_time)
                    except Exception:
                        ask_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                qa_info = {
                    "问题ID": str(item.get("askId", "")),
                    "提问内容": self._clean_text(item.get("title", "")),
                    "提问时间": ask_time,
                    "提问人": item.get("nickName", "热心网友"),
                    "回答数量": item.get("replyCount", 0),  # 使用API返回的replyCount
                    "回答内容": " ".join(formatted_replies) if formatted_replies else ""
                }
                
                # 只添加有效的问答（至少有问题ID和提问内容）
                if qa_info["问题ID"] and qa_info["提问内容"]:
                    qa_list.append(qa_info)

            logger.info(f"解析到 {len(qa_list)} 条问答")
            return qa_list

        except Exception as e:
            logger.error(f"解析问答数据失败: {str(e)}\n{traceback.format_exc()}")
            return []

    def _parse_comment_info(self, comment_data: Dict) -> Dict:
        """解析单条评论信息"""
      
        try:
            user_info = comment_data.get("userInfo", {}) or {}
            grade_info = user_info.get("gradeInfo", {}) or {}
            level_info = user_info.get("levelInfo", {}) or {}

            comment_info = {
                "评论ID": str(comment_data.get("id", "")),
                "用户名": self._clean_text(user_info.get("nickName", "")),
                "用户等级": level_info.get("name", ""),
                "点评身份": grade_info.get("title", ""),
                "评分": comment_data.get("rating", 0),
                "评论内容": self._clean_text(comment_data.get("content", "")),
                "入住时间": comment_data.get("checkin", ""),
                "房型": comment_data.get("roomName", ""),
                "出行类型": comment_data.get("travelTypeText", ""),
                "评论来源": self._get_comment_source(comment_data),
                "有用数": comment_data.get("usefulCount", 0),
                "IP归属地": comment_data.get("ipLocation", ""),
                "评论图片": "",
                "酒店回复": "",
                "回复时间": "",
            }

            # 处理评论图片
            image_urls = []
            if "imageCuttingsList" in comment_data and comment_data["imageCuttingsList"]:
                for img in comment_data["imageCuttingsList"]:
                    # 获取大图URL
                    big_url = img.get("bigImageUrl", "")
                    if big_url:
                        image_urls.append(big_url.split("?")[0])
                
                if image_urls:
                    comment_info["评论图片"] = ",".join(image_urls)

            # 处理酒店回复
            feedback_list = comment_data.get("feedbackList", [])
            if (
                feedback_list
                and isinstance(feedback_list, list)
                and len(feedback_list) > 0
            ):
                first_feedback = feedback_list[0]
                comment_info["酒店回复"] = self._clean_text(
                    first_feedback.get("content", "")
                )
                comment_info["回复时间"] = first_feedback.get("createTime", "")

            logger.info(f"解析评论: {comment_info['评论ID']}")
            return comment_info

        except Exception as e:
            logger.error(f"解析评论信息失败: {str(e)}\n{traceback.format_exc()}")
            return {}

    def _clean_text(self, text: str) -> str:
        """清理文本内容"""
        try:
            if not text:
                return ""
            text = text.replace("\n", " ").replace("\r", " ")
            text = " ".join(text.split())
            return text.strip()
        except Exception as e:
            logger.error(f"清理文本失败: {str(e)}\n{traceback.format_exc()}")
            return text

    def _get_comment_source(self, comment_data: Dict) -> str:
        """获取评论来源"""
        try:
            source = comment_data.get("source", 0)
            source_logo = comment_data.get("sourceLogoUrl", "")

            if source == 1:
                return "Trip.com"
            elif source == 36:
                if "0233v" in source_logo:
                    return "Expedia"
                elif "0232r" in source_logo:
                    return "Hotels.com"
                return "其他国际平台"
            else:
                return "携程"
        except Exception as e:
            logger.error(f"获取评论来源失败: {str(e)}\n{traceback.format_exc()}")
            return "未知来源"

    def _parse_date(self, date_str: str) -> str:
        """解析日期字符串"""
        try:
            if not date_str:
                return ""
            timestamp = int(date_str.split("(")[1].split("+")[0])
            dt = datetime.fromtimestamp(timestamp / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"解析日期失败: {str(e)}\n{traceback.format_exc()}")
            return date_str

    def save_hotel(self, hotel_info: Dict) -> bool:
        """保存酒店数据
        
        Args:
            hotel_info: 酒店信息字典
        Returns:
            bool: 保存是否成功
        """
        try:
            # 构建数据库字段映射
            hotel_db_data = {
                "hotel_id": hotel_info["酒店ID"],
                "name": hotel_info["酒店名称"],
                "name_en": hotel_info["酒店英文名称"],
                "address": hotel_info["详细地址"],
                "location_desc": hotel_info["位置描述"],
                "longitude": hotel_info["经度"],
                "latitude": hotel_info["纬度"],
                "star": hotel_info["星级"],
                "tags": hotel_info["酒店标签"],
                "one_sentence_comment": hotel_info["一句话点评"],
                "rating_all": hotel_info.get("总评分", 0),
                "rating_location": hotel_info.get("环境评分", 0),
                "rating_facility": hotel_info.get("设施评分", 0),
                "rating_service": hotel_info.get("服务评分", 0),
                "rating_room": hotel_info.get("卫生评分", 0),
                "comment_count": hotel_info.get("总评论数", 0),
                "comment_tags": hotel_info.get("评论标签", ""),
                "good_comment_count": hotel_info.get("好评数", 0),
                "bad_comment_count": hotel_info.get("差评数", 0),
                "good_rate": hotel_info.get("好评率", 0),
                "ai_comment": hotel_info.get("AI点评", ""),
                "ai_detailed_comment": hotel_info.get("AI详评", "")
            }

            # 检查酒店是否存在
            hotel = CtripHotel.get_by_id_or_none(hotel_db_data["hotel_id"])
            if hotel:
                # 如果存在，更新数据
                return hotel.update_hotel(hotel_db_data)
            else:
                # 如果不存在，创建新记录
                hotel = CtripHotel.create_hotel(hotel_db_data)
                return hotel is not None

        except Exception as e:
            logger.error(f"保存酒店数据失败: {str(e)}")
            return False

    def save_comment(self, comment_info: Dict, hotel_id: str) -> bool:
        """保存评论数据
        
        Args:
            comment_info: 评论信息字典
            hotel_id: 酒店ID
        Returns:
            bool: 保存是否成功
        """
        try:
            # 获取关联的酒店对象
            hotel = CtripHotel.get_by_id_or_none(hotel_id)
            if not hotel:
                logger.error(f"保存评论失败: 酒店ID {hotel_id} 不存在")
                return False

            # 构建评论数据库字段映射
            comment_db_data = {
                "comment_id": comment_info["评论ID"],
                "hotel": hotel,
                "user_name": comment_info["用户名"],
                "user_level": comment_info["用户等级"],
                "user_identity": comment_info["点评身份"],
                "rating": comment_info["评分"],
                "content": comment_info["评论内容"],
                "checkin_time": comment_info["入住时间"],
                "room_type": comment_info["房型"],
                "travel_type": comment_info["出行类型"],
                "source": comment_info["评论来源"],
                "useful_count": comment_info["有用数"],
                "ip_location": comment_info["IP归属地"],
                "images": comment_info["评论图片"],
                "hotel_reply": comment_info["酒店回复"],
                "reply_time": comment_info["回复时间"]
            }

            # 检查评论是否已存在
            existing_comment = CtripComment.get_by_id_or_none(comment_db_data["comment_id"])
            if existing_comment:
                # 如果存在，更新数据
                return existing_comment.update_comment(comment_db_data)
            else:
                # 如果不存在，创建新记录
                new_comment = CtripComment.create_comment(comment_db_data)
                return new_comment is not None

        except Exception as e:
            logger.error(f"保存评论数据失败: {str(e)}")
            return False

    def save_qa(self, qa_info: Dict, hotel_id: str) -> bool:
        """保存问答数据
        
        Args:
            qa_info: 问答信息字典
            hotel_id: 酒店ID
        Returns:
            bool: 保存是否成功
        """
        try:
            # 获取关联的酒店对象
            hotel = CtripHotel.get_by_id_or_none(hotel_id)
            if not hotel:
                logger.error(f"保存问答失败: 酒店ID {hotel_id} 不存在")
                return False

            # 构建问答数据库字段映射
            qa_db_data = {
                "qa_id": qa_info["问题ID"],
                "hotel": hotel,
                "question": qa_info["提问内容"],
                "ask_time": datetime.strptime(qa_info["提问时间"], "%Y-%m-%d %H:%M:%S"),
                "asker": qa_info["提问人"],
                "reply_count": qa_info["回答数量"],
                "replies": qa_info["回答内容"]
            }

            # 检查问答是否已存在
            existing_qa = CtripQA.get_by_id_or_none(qa_db_data["qa_id"])
            if existing_qa:
                # 如果存在，更新数据
                return existing_qa.update_qa(qa_db_data)
            else:
                # 如果不存在，创建新记录
                new_qa = CtripQA.create_qa(qa_db_data)
                return new_qa is not None

        except Exception as e:
            logger.error(f"保存问答数据失败: {str(e)}")
            return False

    def _process_comment_page(self, comment_data: Dict, hotel_id: str) -> int:
        """处理单页评论数据
        
        Args:
            comment_data: 评论页面数据
            hotel_id: 酒店ID
        Returns:
            int: 成功保存的评论数量
        """
        success_count = 0
        try:
            comments = self._parse_comments(comment_data)
            for comment in comments:
                comment["酒店ID"] = hotel_id
                if self.save_comment(comment, hotel_id):
                    success_count += 1
        except Exception as e:
            logger.error(f"处理评论页面失败: {str(e)}")
        return success_count

    def _get_valid_comments(self, hotel_id: str) -> List[Dict]:
        """获取有效评论用于生成AI点评
        
        Args:
            hotel_id: 酒店ID
        Returns:
            List[Dict]: 有效评论列表
        """
        try:
            hotel = CtripHotel.get_by_id_or_none(hotel_id)
            if not hotel:
                return []
            
            comments = hotel.get_comments()
            valid_comments = []
            for comment in comments:
                if not comment.content:
                    continue
                if not comment.rating:
                    continue
                if len(comment.content.encode("utf-8")) > 100:
                    valid_comments.append({
                        "评论内容": comment.content,
                        "评分": comment.rating,
                        "用户名": comment.user_name,
                        "入住时间": comment.checkin_time,
                        "房型": comment.room_type
                    })
            return valid_comments
        except Exception as e:
            logger.error(f"获取有效评论失败: {str(e)}")
            return []

    def _parse_hotel_info(self, hotel_data: Dict) -> Dict:
        """解析酒店基本信息"""
        try:
            base_info = hotel_data.get("hotelName", "")
            name_default = hotel_data.get("hotelNameDefault", "")
            poi_info = hotel_data.get("poiInfo", {})
            star_info = hotel_data.get("hotelStar", {})
            position_info = hotel_data.get("positionInfo", [{}])[0]

            # 提取标签
            tags = []
            if "tagGroups" in hotel_data and "hotelCard" in hotel_data["tagGroups"]:
                tags = [
                    tag.get("title", "") for tag in hotel_data["tagGroups"]["hotelCard"]
                ]

            # 提取一句话评论
            one_sentence_comment = ""
            if (
                "tagGroups" in hotel_data
                and "oneSentenceComment" in hotel_data["tagGroups"]
            ):
                one_sentence_comment = hotel_data["tagGroups"][
                    "oneSentenceComment"
                ].get("title", "")

            # 获取位置描述
            position_desc = ""
            if position_info:
                city_name = poi_info.get("cityName", "")
                near_info = position_info.get("positionDesc", "")
                if city_name and near_info:
                    position_desc = f"{city_name} · {near_info}"
                else:
                    position_desc = near_info or city_name

            # 构建酒店信息
            hotel_info = {
                "酒店ID": str(hotel_data.get("hotelId", "")),
                "酒店名称": self._clean_text(base_info),
                "酒店英文名称": name_default,
                "详细地址": poi_info.get("positionDesc", ""),
                "位置描述": position_desc,
                "经度": poi_info.get("coordinate", {}).get("longitude", ""),
                "纬度": poi_info.get("coordinate", {}).get("latitude", ""),
                "星级": star_info.get("star", 0),
                "酒店标签": ",".join(filter(None, tags)),
                "一句话点评": one_sentence_comment,
                "AI点评": "",
                "AI详评": "",
            }
            logger.info(f"解析酒店信息: {hotel_info['酒店名称']}")
            return hotel_info

        except Exception as e:
            logger.error(f"解析酒店信息失败: {str(e)}\n{traceback.format_exc()}")
            return {}

    def _get_hotel_rating(self, comment_data: Dict) -> Dict:
        """获取酒店评分详细信息"""
        try:
            # 从评论数据中获取评分信息
            comment_rating = comment_data.get("commentRating", {})
            comment_tags = comment_data.get("commentTagList", []) or []
            statistic_list = comment_data.get("statisticList", []) or []
            
            # 提取评论标签
            tag_str = " / ".join(
                f"{tag['name']} {tag['commentCount']}"
                for tag in comment_tags
                if tag.get("name") and tag.get("commentCount")
            )

            # 构建评分信息
            rating_info = {
                "总评分": comment_rating.get("ratingAll", 0),
                "环境评分": comment_rating.get("ratingLocation", 0),
                "设施评分": comment_rating.get("ratingFacility", 0),
                "服务评分": comment_rating.get("ratingService", 0),
                "卫生评分": comment_rating.get("ratingRoom", 0),
                "总评论数": comment_rating.get("showCommentNum", 0),
                "评论标签": tag_str,
                "好评数": 0,
                "差评数": 0,
                "好评率": 0
            }

            # 获取好评和差评数量
            for stat in statistic_list:
                stat_name = stat.get("name", "")
                if stat_name == "值得推荐":
                    rating_info["好评数"] = stat.get("commentCount", 0)
                elif stat_name == "差评":
                    rating_info["差评数"] = stat.get("commentCount", 0)

            # 计算好评率
            total_reviews = rating_info["好评数"] + rating_info["差评数"]
            if total_reviews > 0:
                rating_info["好评率"] = round(rating_info["好评数"] / total_reviews * 100, 1)

            logger.info(f"获取评分信息成功: {rating_info}")
            return rating_info

        except Exception as e:
            logger.error(f"获取酒店评分信息失败: {str(e)}\n{traceback.format_exc()}")
            return {}

    def _parse_comments(self, comment_data: Dict) -> List[Dict]:
        """解析评论数据"""
        try:
            comments = []
            comment_groups = comment_data.get("groupList", [])

            for group in comment_groups:
                comment_list = group.get("commentList", [])
                for comment in comment_list:
                    comment_info = self._parse_comment_info(comment)
                    if comment_info:
                        comments.append(comment_info)

            return comments

        except Exception as e:
            logger.error(f"解析评论数据失败: {str(e)}\n{traceback.format_exc()}")
            return []
    
    def process_hotel(self, hotel_data: Dict) -> bool:
        """处理单个酒店数据"""
        try:
            # 1. 解析酒店基本信息
            hotel_info = self._parse_hotel_info(hotel_data)
            if not hotel_info:
                return False

            hotel_id = hotel_info["酒店ID"]
            
            # 2. 获取并处理评论数据
            first_page_comments = self.get_hotel_comments(hotel_id, 1)
            if first_page_comments:
                # 从第一页评论中获取评分信息
                rating_info = self._get_hotel_rating(first_page_comments)
                if rating_info:
                    hotel_info.update(rating_info)
                    logger.info(f"获取酒店评分信息: {hotel_info['酒店ID']}")

                # 处理所有评论
                total_comments = first_page_comments.get("totalCountForPage", 0)
                if total_comments > 0:
                    self.total_comments += total_comments
                    comment_pages = (total_comments + 9) // 10
                    logger.info(f"开始获取评论: 共 {total_comments} 条，分 {comment_pages} 页")
                    
                    # 处理第一页评论
                    comment_success = self._process_comment_page(first_page_comments, hotel_id)
                    
                    # 处理剩余页面
                    if total_comments > 10:
                        for page in range(2, comment_pages + 1):
                            logger.info(f"正在获取第 {page}/{comment_pages} 页评论...")
                            result = self.get_hotel_comments(hotel_id, page)
                            if not result:
                                logger.error(f"获取第 {page} 页评论失败")
                                break
                            
                            page_success = self._process_comment_page(result, hotel_id)
                            comment_success += page_success
                            
                            logger.info(f"第 {page} 页评论处理完成: 成功保存 {page_success} 条")
                    
                    self.saved_comments += comment_success
                    logger.info(f"评论处理完成: 成功保存 {comment_success}/{total_comments} 条")

            # 3. 获取并处理问答数据
            qa_data = self.get_hotel_qa(hotel_id)
            if qa_data:
                qa_list = self._parse_qa(qa_data)
                qa_success = 0
                for qa in qa_list:
                    qa["酒店ID"] = hotel_id
                    if self.save_qa(qa, hotel_id):
                        qa_success += 1
                logger.info(f"问答处理完成: 成功保存 {qa_success}/{len(qa_list)} 条")

            # 4. 生成AI点评
            if self.saved_comments > 0:
                valid_comments = self._get_valid_comments(hotel_id)
                if valid_comments:
                    try:
                        hotel_info["AI点评"] = self.ai_generator.generate_comment(hotel_info, valid_comments)
                        hotel_info["AI详评"] = self.ai_generator.generate_detailed_comment(hotel_info, valid_comments)
                        logger.info(f"生成AI点评成功: {hotel_id}")
                    except Exception as e:
                        logger.error(f"生成AI点评失败: {str(e)}")

            # 5. 保存酒店数据
            return self.save_hotel(hotel_info)

        except Exception as e:
            logger.error(f"处理酒店数据失败: {str(e)}\n{traceback.format_exc()}")
            return False

    def run(self):
        """运行爬虫"""
        try:
            # 获取第一页以确定总数
            first_result = self.get_hotel_list(page=1)
            if not first_result or "data" not in first_result:
                logger.error("获取酒店列表失败")
                return

            total_hotels = first_result["data"].get("hotelTotalCount", 0)
            total_pages = (total_hotels + 9) // 10

            logger.info(f"\n{'='*20} 开始数据采集 {'='*20}")
            logger.info(f"总计 {total_hotels} 家酒店，共 {total_pages} 页")

            # 统计变量
            processed_count = 0
            success_count = 0
            failed_count = 0

            # 遍历所有页面
            for current_page in range(1, total_pages + 1):
                logger.info(f"\n{'='*15} 正在获取第 {current_page}/{total_pages} 页酒店列表 {'='*15}")

                result = self.get_hotel_list(page=current_page)
                if (
                    not result
                    or "data" not in result
                    or "hotelList" not in result["data"]
                ):
                    logger.error(f"获取第 {current_page} 页酒店列表失败")
                    continue

                hotels = result["data"]["hotelList"]
                if not hotels:
                    logger.warning(f"第 {current_page} 页没有酒店数据")
                    continue

                logger.info(f"第 {current_page} 页共有 {len(hotels)} 家酒店")

                # 处理当前页的所有酒店
                for hotel_data in hotels:
                    try:
                        hotel_name = hotel_data.get("hotelName", "未知酒店")
                        logger.info(f"\n{'='*10} 开始处理酒店: {hotel_name} {'='*10}")

                        if self.process_hotel(hotel_data):
                            logger.info(f"处理酒店成功: {hotel_name}")
                            success_count += 1
                        else:
                            logger.error(f"处理酒店失败: {hotel_name}")
                            failed_count += 1

                        processed_count += 1
                        progress = (processed_count/total_hotels*100)
                        logger.info(f"\n总进度: {processed_count}/{total_hotels} ({progress:.1f}%)")
                        logger.info(f"统计: 成功 {success_count} 家，失败 {failed_count} 家，评论 {self.saved_comments}/{self.total_comments} 条")

                    except Exception as e:
                        logger.error(f"处理酒店数据时出错: {str(e)}\n{traceback.format_exc()}")
                        failed_count += 1
                        continue

            logger.info(f"\n{'='*20} 数据采集完成 {'='*20}")
            logger.info("统计信息:")
            logger.info(f"酒店: 总数 {total_hotels}，成功 {success_count}，失败 {failed_count}")
            logger.info(f"评论: 总数 {self.total_comments}，成功保存 {self.saved_comments}")
            logger.info(f"{'='*50}\n")

        except KeyboardInterrupt:
            logger.info("\n程序被用户中断")
        except Exception as e:
            logger.error(f"程序执行出错: {str(e)}\n{traceback.format_exc()}")
        finally:
            logger.info("数据采集完成")


def main():
    """主函数"""
    spider = CtripSpider(country_id=66, province_id=10094, city_id=-1)
    spider.run()


if __name__ == "__main__":
    main()

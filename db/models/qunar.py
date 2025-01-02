"""去哪儿酒店数据模型"""

from datetime import datetime
from utils.logger import setup_logger
from typing import Dict, List, Optional
from peewee import CharField, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField
from ..connection import BaseModel

logger = setup_logger(__name__)

class QunarHotel(BaseModel):
    """去哪儿酒店模型"""
    hotel_id = CharField(primary_key=True)
    name = CharField()                           # 酒店名称
    level = CharField(null=True)                 # 酒店等级（钻石/舒适型等）
    address = TextField(null=True)               # 详细地址
    location = TextField(null=True)              # 位置描述（如：近东门老街 · 罗湖口岸/火车站）
    longitude = CharField(null=True)             # 经度
    latitude = CharField(null=True)              # 纬度
    score = FloatField(null=True)               # 总评分
    score_service = FloatField(null=True)        # 服务评分
    score_location = FloatField(null=True)       # 位置评分
    score_facility = FloatField(null=True)       # 设施评分
    score_hygiene = FloatField(null=True)       # 卫生评分
    facilities = TextField(null=True)            # 设备设施信息（所有类型）
    highlight = TextField(null=True)             # 一句话亮点
    ai_summary = TextField(null=True)            # AI 总结（从评论接口获取）
    comment_count = IntegerField(null=True)      # 评论总数
    comment_tags = TextField(null=True)          # 评论标签（如：服务贴心(3234) / 环境舒适(897)）
    good_rate = CharField(null=True)             # 好评率（如：97%好评）
    ranking = TextField(null=True)               # 排名信息（如：深圳站舒适酒店榜第1名）
    open_time = CharField(null=True)             # 开业时间
    renovation_time = CharField(null=True)       # 装修时间
    phone = CharField(null=True)                 # 联系电话
    location_advantage = TextField(null=True)    # 位置优势（如：距离向西村地铁站119米）
    traffic = TextField(null=True)               # 交通信息（所有类型）
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "qunar_hotels"

    @classmethod
    def get_or_none(cls, hotel_id: str) -> Optional['QunarHotel']:
        """根据ID获取酒店，不存在返回None"""
        try:
            return cls.get_by_id(hotel_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_from_list(cls, hotel_info: Dict) -> 'QunarHotel':
        """从列表数据创建酒店"""
        return cls.create(
            hotel_id=hotel_info['id'],
            name=hotel_info['name'],
            level=hotel_info['level'],
            location=hotel_info['location'],
            longitude=str(hotel_info['longitude']),
            latitude=str(hotel_info['latitude']),
            score=hotel_info['score'],
            comment_count=hotel_info['comment_count'],
            highlight=hotel_info['highlight'],
            updated_at=datetime.now()
        )

    def update_from_detail(self, detail_info: Dict) -> bool:
        """从详情数据更新酒店"""
        try:
            self.address = detail_info.get('address')
            self.open_time = detail_info.get('open_time')
            self.renovation_time = detail_info.get('renovation_time')
            self.phone = detail_info.get('phone')
            self.comment_tags = detail_info.get('comment_tags')
            self.ranking = detail_info.get('ranking')
            self.good_rate = detail_info.get('good_rate')
            self.location_advantage = detail_info.get('location_advantage')
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新酒店详情失败: {str(e)}")
            return False

    def update_scores(self, scores: Dict) -> bool:
        """更新酒店评分"""
        try:
            self.score_service = float(scores.get('服务', 0))
            self.score_location = float(scores.get('位置', 0))
            self.score_facility = float(scores.get('设施', 0))
            self.score_hygiene = float(scores.get('卫生', 0))
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新酒店评分失败: {str(e)}")
            return False

    def update_traffic(self, traffic_data: Dict) -> bool:
        """更新酒店交通信息"""
        try:
            traffic_list = []
            type_names = {
                'subway': '地铁',
                'airport': '机场',
                'railway': '火车站',
                'bus': '汽车站',
                'landmark': '地标'
            }
            
            for traffic_type, type_name in type_names.items():
                items = traffic_data.get(traffic_type, [])
                if items:
                    info_list = [f"{item['name']}({item['distance']})" for item in items]
                    traffic_list.append(f"{type_name}：{' / '.join(info_list)}")
            
            self.traffic = ' | '.join(traffic_list) if traffic_list else None
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新酒店交通信息失败: {str(e)}")
            return False

    def update_facilities(self, facilities: Dict) -> bool:
        """更新酒店设施信息"""
        try:
            facility_list = []
            for category, items in facilities.items():
                if items:
                    facility_list.append(f"{category}：{' / '.join(items)}")
            
            self.facilities = ' | '.join(facility_list) if facility_list else None
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新酒店设施信息失败: {str(e)}")
            return False

    def update_ai_summary(self, summary: str) -> bool:
        """更新AI总结"""
        try:
            self.ai_summary = summary
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新AI总结失败: {str(e)}")
            return False

    def get_comments(self, limit: int = None) -> List['QunarComment']:
        """获取酒店评论列表"""
        query = self.comments.order_by(QunarComment.feed_time.desc())
        if limit:
            query = query.limit(limit)
        return list(query)

    def get_qas(self, limit: int = None) -> List['QunarQA']:
        """获取酒店问答列表"""
        query = self.qas.order_by(QunarQA.ask_time.desc())
        if limit:
            query = query.limit(limit)
        return list(query)

class QunarComment(BaseModel):
    """去哪儿网评论模型"""
    
    comment_id = CharField(primary_key=True)
    hotel = ForeignKeyField(QunarHotel, backref='comments')
    user_name = CharField()                      # 用户名
    rating = FloatField()                        # 评分
    content = TextField()                        # 评论内容
    checkin_time = CharField(null=True)          # 入住时间（年月）
    room_type = CharField(null=True)             # 房型
    travel_type = CharField(null=True)           # 出行类型
    source = CharField(null=True)                # 评论来源
    ip_location = CharField(null=True)           # IP归属地
    images = TextField(null=True)                # 评论图片URL列表
    image_count = IntegerField(default=0)        # 图片数量
    like_count = IntegerField(default=0)         # 点赞数
    reply_content = TextField(null=True)         # 酒店回复内容
    reply_time = CharField(null=True)            # 回复时间
    feed_time = DateTimeField(null=True)         # 评论发布时间
    created_at = DateTimeField(default=datetime.now)
    
    class Meta:
        table_name = "qunar_comments"

    @classmethod
    def get_or_none(cls, comment_id: str) -> Optional['QunarComment']:
        """根据ID获取评论，不存在返回None"""
        try:
            return cls.get_by_id(comment_id)
        except cls.DoesNotExist:
            return None
    
    @classmethod
    def create_from_api(cls, comment_info: Dict, hotel: QunarHotel) -> 'QunarComment':
        """从API数据创建评论"""
        return cls.create(
            comment_id=comment_info['comment_id'],
            hotel=hotel,
            user_name=comment_info['user_name'],
            rating=comment_info['rating'],
            content=comment_info['content'],
            checkin_time=comment_info.get('checkin_time'),
            room_type=comment_info.get('room_type'),
            travel_type=comment_info.get('travel_type'),
            source=comment_info.get('source'),
            ip_location=comment_info.get('ip_location'),
            images=comment_info.get('images'),
            image_count=comment_info.get('image_count', 0),
            like_count=comment_info.get('like_count', 0),
            reply_content=comment_info.get('reply_content'),
            reply_time=comment_info.get('reply_time'),
            feed_time=comment_info.get('feed_time')
        )
    
    def update_reply(self, content: str, time: str) -> bool:
        """更新酒店回复"""
        try:
            self.reply_content = content
            self.reply_time = time
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新评论回复失败: {str(e)}")
            return False
    
    def update_likes(self, count: int) -> bool:
        """更新点赞数"""
        try:
            self.like_count = count
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新评论点赞数失败: {str(e)}")
            return False
    
    def get_images(self) -> List[str]:
        """获取评论图片列表"""
        return self.images.split(',') if self.images else []


class QunarQA(BaseModel):
    """去哪儿问答模型"""
    qa_id = CharField(primary_key=True)
    hotel = ForeignKeyField(QunarHotel, backref='qas')
    question = TextField()                       # 问题内容
    ask_time = DateTimeField()                  # 提问时间
    asker = CharField()                         # 提问者
    reply_count = IntegerField(default=0)       # 回复数量
    replies = TextField(null=True)              # 回复内容列表
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "qunar_qas"

    @classmethod
    def get_or_none(cls, qa_id: str) -> Optional['QunarQA']:
        """根据ID获取问答，不存在返回None"""
        try:
            return cls.get_by_id(qa_id)
        except cls.DoesNotExist:
            return None
    
    @classmethod
    def create_from_api(cls, qa_info: Dict, hotel: QunarHotel) -> 'QunarQA':
        """从API数据创建问答"""
        return cls.create(
            qa_id=qa_info['qa_id'],
            hotel=hotel,
            question=qa_info['question'],
            ask_time=qa_info['ask_time'],
            asker=qa_info['asker'],
            reply_count=qa_info.get('reply_count', 0),
            replies=qa_info.get('replies')
        )

    def update_replies(self, replies: List[str]) -> bool:
        """更新问答回复"""
        try:
            formatted_replies = []
            for idx, reply in enumerate(replies, 1):
                formatted_replies.append(f"{idx}. {reply}")
            
            self.replies = ' '.join(formatted_replies) if formatted_replies else None
            self.reply_count = len(replies)
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新问答回复失败: {str(e)}")
            return False
    
    def get_replies(self) -> List[str]:
        """获取回复列表"""
        if not self.replies:
            return []
        return [reply.split('. ', 1)[1] for reply in self.replies.split(' ') if '. ' in reply] 
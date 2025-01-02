"""携程酒店数据模型"""

from datetime import datetime
from utils.logger import setup_logger
from typing import Dict, List, Optional
from peewee import CharField, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField
from ..connection import BaseModel

logger = setup_logger(__name__)

class CtripHotel(BaseModel):
    """携程酒店模型"""
    hotel_id = CharField(primary_key=True)
    name = CharField()
    name_en = CharField(null=True)
    address = TextField(null=True)
    location_desc = TextField(null=True)
    longitude = CharField(null=True)
    latitude = CharField(null=True)
    star = IntegerField(null=True)
    tags = TextField(null=True)
    one_sentence_comment = TextField(null=True)
    ai_comment = TextField(null=True)
    ai_detailed_comment = TextField(null=True)
    rating_all = FloatField(null=True)
    rating_location = FloatField(null=True)
    rating_facility = FloatField(null=True)
    rating_service = FloatField(null=True)
    rating_room = FloatField(null=True)
    comment_count = IntegerField(null=True)
    comment_tags = TextField(null=True)
    good_comment_count = IntegerField(null=True)
    bad_comment_count = IntegerField(null=True)
    good_rate = FloatField(null=True)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "ctrip_hotels"

    @classmethod
    def get_or_none(cls, hotel_id: str) -> Optional['CtripHotel']:
        """根据ID获取酒店，不存在返回None"""
        try:
            return cls.get_by_id(hotel_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_from_api(cls, hotel_info: Dict) -> 'CtripHotel':
        """从API数据创建酒店"""
        return cls.create(
            hotel_id=hotel_info['酒店ID'],
            name=hotel_info['酒店名称'],
            name_en=hotel_info['酒店英文名称'],
            address=hotel_info['详细地址'],
            location_desc=hotel_info['位置描述'],
            longitude=hotel_info['经度'],
            latitude=hotel_info['纬度'],
            star=hotel_info['星级'],
            tags=hotel_info['酒店标签'],
            one_sentence_comment=hotel_info['一句话点评'],
            updated_at=datetime.now()
        )

    def update_ratings(self, rating_info: Dict) -> bool:
        """更新酒店评分"""
        try:
            self.rating_all = rating_info.get('总评分', 0)
            self.rating_location = rating_info.get('环境评分', 0)
            self.rating_facility = rating_info.get('设施评分', 0)
            self.rating_service = rating_info.get('服务评分', 0)
            self.rating_room = rating_info.get('卫生评分', 0)
            self.comment_count = rating_info.get('总评论数', 0)
            self.comment_tags = rating_info.get('评论标签', '')
            self.good_comment_count = rating_info.get('好评数', 0)
            self.bad_comment_count = rating_info.get('差评数', 0)
            self.good_rate = rating_info.get('好评率', 0)
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新酒店评分失败: {str(e)}")
            return False

    def update_ai_comments(self, ai_comment: str, ai_detailed_comment: str) -> bool:
        """更新AI点评"""
        try:
            self.ai_comment = ai_comment
            self.ai_detailed_comment = ai_detailed_comment
            self.updated_at = datetime.now()
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新AI点评失败: {str(e)}")
            return False

    def get_comments(self, limit: int = None) -> List['CtripComment']:
        """获取酒店评论列表"""
        query = self.comments.order_by(CtripComment.created_at.desc())
        if limit:
            query = query.limit(limit)
        return list(query)

    def get_qas(self, limit: int = None) -> List['CtripQA']:
        """获取酒店问答列表"""
        query = self.qas.order_by(CtripQA.ask_time.desc())
        if limit:
            query = query.limit(limit)
        return list(query)

class CtripComment(BaseModel):
    """携程评论模型"""
    comment_id = CharField(primary_key=True)
    hotel = ForeignKeyField(CtripHotel, backref='comments')
    user_name = CharField()
    user_level = CharField(null=True)
    user_identity = CharField(null=True)
    rating = FloatField()
    content = TextField()
    checkin_time = CharField(null=True)
    room_type = CharField(null=True)
    travel_type = CharField(null=True)
    source = CharField(null=True)
    useful_count = IntegerField(default=0)
    ip_location = CharField(null=True)
    images = TextField(null=True)
    hotel_reply = TextField(null=True)
    reply_time = CharField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "ctrip_comments"   

    @classmethod
    def get_or_none(cls, comment_id: str) -> Optional['CtripComment']:
        """根据ID获取评论，不存在返回None"""
        try:
            return cls.get_by_id(comment_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_from_api(cls, comment_info: Dict, hotel: CtripHotel) -> 'CtripComment':
        """从API数据创建评论"""
        return cls.create(
            comment_id=comment_info['评论ID'],
            hotel=hotel,
            user_name=comment_info['用户名'],
            user_level=comment_info['用户等级'],
            user_identity=comment_info['点评身份'],
            rating=comment_info['评分'],
            content=comment_info['评论内容'],
            checkin_time=comment_info['入住时间'],
            room_type=comment_info['房型'],
            travel_type=comment_info['出行类型'],
            source=comment_info['评论来源'],
            useful_count=comment_info['有用数'],
            ip_location=comment_info['IP归属地'],
            images=comment_info['评论图片'],
            hotel_reply=comment_info['酒店回复'],
            reply_time=comment_info['回复时间']
        )

    def update_useful_count(self, count: int) -> bool:
        """更新点赞数"""
        try:
            self.useful_count = count
            self.save()
            return True
        except Exception as e:
            logger.error(f"更新评论点赞数失败: {str(e)}")
            return False

    def get_images(self) -> List[str]:
        """获取评论图片列表"""
        return self.images.split(',') if self.images else []

class CtripQA(BaseModel):
    """携程问答模型"""
    qa_id = CharField(primary_key=True)
    hotel = ForeignKeyField(CtripHotel, backref='qas')
    question = TextField()
    ask_time = DateTimeField()
    asker = CharField()
    reply_count = IntegerField(default=0)
    replies = TextField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "ctrip_qas"

    @classmethod
    def get_or_none(cls, qa_id: str) -> Optional['CtripQA']:
        """根据ID获取问答，不存在返回None"""
        try:
            return cls.get_by_id(qa_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_from_api(cls, qa_info: Dict, hotel: CtripHotel) -> 'CtripQA':
        """从API数据创建问答"""
        return cls.create(
            qa_id=qa_info['问题ID'],
            hotel=hotel,
            question=qa_info['提问内容'],
            ask_time=datetime.strptime(qa_info['提问时间'], "%Y-%m-%d %H:%M:%S"),
            asker=qa_info['提问人'],
            reply_count=qa_info['回答数量'],
            replies=qa_info['回答内容']
        )

    def get_replies(self) -> List[str]:
        """获取回复列表"""
        if not self.replies:
            return []
        return [reply.split('.', 1)[1] for reply in self.replies.split(' ') if '.' in reply] 
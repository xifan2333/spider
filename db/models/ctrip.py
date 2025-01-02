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
    def get_by_id_or_none(cls, hotel_id: str) -> Optional['CtripHotel']:
        """根据ID获取酒店，不存在返回None"""
        try:
            return cls.get_by_id(hotel_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_hotel(cls, hotel_data: Dict) -> Optional['CtripHotel']:
        """创建酒店记录
        
        Args:
            hotel_data: 酒店数据字典
            
        Returns:
            Optional[CtripHotel]: 创建的酒店实例,失败返回None
        """
        try:
            hotel = cls.create(**hotel_data)
            logger.info(f"创建酒店成功: {hotel_data.get('hotel_id')}")
            return hotel
        except Exception as e:
            logger.error(f"创建酒店失败: {str(e)}")
            return None

    def update_hotel(self, hotel_data: Dict) -> bool:
        """更新酒店记录
        
        Args:
            hotel_data: 酒店数据字典
            
        Returns:
            bool: 更新是否成功
        """
        try:
            hotel_data["updated_at"] = datetime.now()
            for key, value in hotel_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新酒店成功: {self.hotel_id}")
            return True
        except Exception as e:
            logger.error(f"更新酒店失败: {str(e)}")
            return False

    def delete_hotel(self) -> bool:
        """删除酒店记录
        
        Returns:
            bool: 删除是否成功
        """
        try:
            self.delete_instance()
            logger.info(f"删除酒店成功: {self.hotel_id}")
            return True
        except Exception as e:
            logger.error(f"删除酒店失败: {str(e)}")
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
    def get_by_id_or_none(cls, comment_id: str) -> Optional['CtripComment']:
        """根据ID获取评论，不存在返回None"""
        try:
            return cls.get_by_id(comment_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_comment(cls, comment_data: Dict) -> Optional['CtripComment']:
        """创建评论记录
        
        Args:
            comment_data: 评论数据字典
            
        Returns:
            Optional[CtripComment]: 创建的评论实例,失败返回None
        """
        try:
            comment = cls.create(**comment_data)
            logger.info(f"创建评论成功: {comment_data.get('comment_id')}")
            return comment
        except Exception as e:
            logger.error(f"创建评论失败: {str(e)}")
            return None

    def update_comment(self, comment_data: Dict) -> bool:
        """更新评论记录
        
        Args:
            comment_data: 评论数据字典
            
        Returns:
            bool: 更新是否成功
        """
        try:
            for key, value in comment_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新评论成功: {self.comment_id}")
            return True
        except Exception as e:
            logger.error(f"更新评论失败: {str(e)}")
            return False

    def delete_comment(self) -> bool:
        """删除评论记录
        
        Returns:
            bool: 删除是否成功
        """
        try:
            self.delete_instance()
            logger.info(f"删除评论成功: {self.comment_id}")
            return True
        except Exception as e:
            logger.error(f"删除评论失败: {str(e)}")
            return False

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
    def get_by_id_or_none(cls, qa_id: str) -> Optional['CtripQA']:
        """根据ID获取问答，不存在返回None"""
        try:
            return cls.get_by_id(qa_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_qa(cls, qa_data: Dict) -> Optional['CtripQA']:
        """创建问答记录
        
        Args:
            qa_data: 问答数据字典
            
        Returns:
            Optional[CtripQA]: 创建的问答实例,失败返回None
        """
        try:
            qa = cls.create(**qa_data)
            logger.info(f"创建问答成功: {qa_data.get('qa_id')}")
            return qa
        except Exception as e:
            logger.error(f"创建问答失败: {str(e)}")
            return None

    def update_qa(self, qa_data: Dict) -> bool:
        """更新问答记录
        
        Args:
            qa_data: 问答数据字典
            
        Returns:
            bool: 更新是否成功
        """
        try:
            for key, value in qa_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新问答成功: {self.qa_id}")
            return True
        except Exception as e:
            logger.error(f"更新问答失败: {str(e)}")
            return False

    def delete_qa(self) -> bool:
        """删除问答记录
        
        Returns:
            bool: 删除是否成功
        """
        try:
            self.delete_instance()
            logger.info(f"删除问答成功: {self.qa_id}")
            return True
        except Exception as e:
            logger.error(f"删除问答失败: {str(e)}")
            return False 
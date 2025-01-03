"""去哪儿网酒店数据模型"""

from datetime import datetime
from typing import Dict, List, Optional
from peewee import CharField, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField, BooleanField
from ..connection import BaseModel
from utils.logger import setup_logger

logger = setup_logger(__name__)

class QunarHotel(BaseModel):
    """去哪儿酒店模型"""
    hotel_id = CharField(primary_key=True)
    name = CharField()
    en_name = CharField(null=True)
    latitude = FloatField(null=True)
    longitude = FloatField(null=True)
    level = CharField(null=True)
    score = FloatField(null=True)
    address = TextField(null=True)
    phone = CharField(null=True)
    comment_count = IntegerField(default=0)
    highlight = TextField(null=True)
    
    # 详细信息
    open_time = CharField(null=True)
    fitment_time = CharField(null=True)
    room_count = IntegerField(null=True)
    comment_tags = TextField(null=True)
    ranking = CharField(null=True)
    good_rate = CharField(null=True)
    location_advantage = TextField(null=True)
    facilities = TextField(null=True)
    traffic_info = TextField(null=True)
    detail_score = TextField(null=True)
    is_platform_choice = BooleanField(default=False)
    
    # AI点评
    ai_comment = TextField(null=True)
    ai_images = TextField(null=True)
    
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "qunar_hotels"

    @classmethod
    def get_by_id_or_none(cls, hotel_id: str) -> Optional['QunarHotel']:
        """根据ID获取酒店,不存在返回None"""
        try:
            return cls.get_by_id(hotel_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_hotel(cls, hotel_data: Dict) -> Optional['QunarHotel']:
        """创建酒店记录"""
        try:
            hotel = cls.create(**hotel_data)
            logger.info(f"创建酒店成功: {hotel_data.get('hotel_id')}")
            return hotel
        except Exception as e:
            logger.error(f"创建酒店失败: {str(e)}")
            return None

    def update_hotel(self, hotel_data: Dict) -> bool:
        """更新酒店记录"""
        try:
            hotel_data["updated_at"] = datetime.now()
            # 过滤掉空值和空字符串
            update_data = {
                key: value for key, value in hotel_data.items()
                if value is not None and value != ""
            }
            
            for key, value in update_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新酒店成功: {self.hotel_id}")
            return True
        except Exception as e:
            logger.error(f"更新酒店失败: {str(e)}")
            return False

    def delete_hotel(self) -> bool:
        """删除酒店记录"""
        try:
            self.delete_instance()
            logger.info(f"删除酒店成功: {self.hotel_id}")
            return True
        except Exception as e:
            logger.error(f"删除酒店失败: {str(e)}")
            return False

    def get_comments(self, limit: int = None) -> List['QunarComment']:
        """获取酒店评论列表"""
        query = self.comments.order_by(QunarComment.comment_time.desc())
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
    """去哪儿评论模型"""
    comment_id = CharField(primary_key=True)
    hotel = ForeignKeyField(QunarHotel, backref='comments')
    username = CharField()
    score = FloatField()
    content = TextField()
    check_in_date = CharField(null=True)
    room_type = CharField(null=True)
    trip_type = CharField(null=True)
    source = CharField(null=True)
    ip_location = CharField(null=True)
    
    images = TextField(null=True)
    image_count = IntegerField(default=0)
    like_count = IntegerField(default=0)
    
    reply_content = TextField(null=True)
    reply_time = CharField(null=True)
    comment_time = DateTimeField()
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "qunar_comments"

    @classmethod
    def get_by_id_or_none(cls, comment_id: str) -> Optional['QunarComment']:
        """根据ID获取评论,不存在返回None"""
        try:
            return cls.get_by_id(comment_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_comment(cls, comment_data: Dict) -> Optional['QunarComment']:
        """创建评论记录"""
        try:
                # 创建记录前检查 ID
            if not comment_data['comment_id']:
                logger.error(f"评论ID为空，无法创建记录: {comment_data}")
                return None
                
            comment = cls.create(**comment_data)
            logger.info(f"创建评论成功: {comment_data['comment_id']}")
            return comment
        except Exception as e:
            logger.error(f"创建评论失败: {str(e)}")
            return None

    def update_comment(self, comment_data: Dict) -> bool:
        """更新评论记录"""
        try:
            # 过滤掉空值和空字符串
            update_data = {
                key: value for key, value in comment_data.items()
                if value is not None and value != ""
            }
            
            for key, value in update_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新评论成功: {self.comment_id}")
            return True
        except Exception as e:
            logger.error(f"更新评论失败: {str(e)}")
            return False

    def delete_comment(self) -> bool:
        """删除评论记录"""
        try:
            self.delete_instance()
            logger.info(f"删除评论成功: {self.comment_id}")
            return True
        except Exception as e:
            logger.error(f"删除评论失败: {str(e)}")
            return False

class QunarQA(BaseModel):
    """去哪儿问答模型"""
    qa_id = CharField(primary_key=True)
    hotel = ForeignKeyField(QunarHotel, backref='qas')
    question = TextField()
    asker_nickname = CharField()
    ask_time = CharField()
    answer_count = IntegerField(default=0)
    question_source = CharField(null=True)
    
    answer_id = CharField(null=True)
    answerer_nickname = CharField(null=True)
    answer_time = CharField(null=True)
    answer_content = TextField(null=True)
    is_official = BooleanField(default=False)
    
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "qunar_qas"

    @classmethod
    def get_by_id_or_none(cls, qa_id: str) -> Optional['QunarQA']:
        """根据ID获取问答,不存在返回None"""
        try:
            return cls.get_by_id(qa_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_qa(cls, qa_data: Dict) -> Optional['QunarQA']:
        """创建问答记录"""
        try:
            # 检查必要字段
            if not qa_data.get("qa_id"):
                logger.error("问答ID为空，无法创建记录")
                return None
            
            qa = cls.create(**qa_data)
            logger.info(f"创建问答成功: {qa_data.get('qa_id')}")
            return qa
        except Exception as e:
            logger.error(f"创建问答失败: {str(e)}")
            return None

    def update_qa(self, qa_data: Dict) -> bool:
        """更新问答记录"""
        try:
            # 过滤掉空值和空字符串
            update_data = {
                key: value for key, value in qa_data.items()
                if value is not None and value != ""
            }
            
            for key, value in update_data.items():
                setattr(self, key, value)
            self.save()
            logger.info(f"更新问答成功: {self.qa_id}")
            return True
        except Exception as e:
            logger.error(f"更新问答失败: {str(e)}")
            return False

    def delete_qa(self) -> bool:
        """删除问答记录"""
        try:
            self.delete_instance()
            logger.info(f"删除问答成功: {self.qa_id}")
            return True
        except Exception as e:
            logger.error(f"删除问答失败: {str(e)}")
            return False

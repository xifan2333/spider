"""艺龙酒店数据模型"""

from datetime import datetime
from peewee import CharField, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField, BooleanField
from ..connection import BaseModel

class ElongHotel(BaseModel):
    """艺龙酒店模型"""
    hotel_id = CharField(primary_key=True)  # 酒店ID
    name = CharField()  # 酒店名称
    name_en = CharField(null=True)  # 英文名称
    address = TextField(null=True)  # 地址
    location = TextField(null=True)  # 位置信息
    star = CharField(null=True)  # 星级
    tags = TextField(null=True)  # 标签
    main_tag = TextField(null=True)  # 主要标签
    traffic_info = TextField(null=True)  # 交通信息
    city_name = CharField(null=True)  # 城市名称
    
    # 评分信息
    score = FloatField(null=True)  # 总评分
    score_service = FloatField(null=True)  # 服务评分
    score_location = FloatField(null=True)  # 位置评分
    score_facility = FloatField(null=True)  # 设施评分
    score_hygiene = FloatField(null=True)  # 卫生评分
    score_cost = FloatField(null=True)  # 性价比评分
    score_desc = TextField(null=True)  # 评分描述
    
    # 评论统计
    comment_count = IntegerField(null=True)  # 总评论数
    good_rate = FloatField(null=True)  # 好评率
    good_count = IntegerField(null=True)  # 好评数
    bad_count = IntegerField(null=True)  # 差评数
    image_count = IntegerField(null=True)  # 图片数量
    video_count = IntegerField(null=True)  # 视频数量
    ai_summary = TextField(null=True)  # AI评论总结
    
    created_at = DateTimeField(default=datetime.now)  # 创建时间
    updated_at = DateTimeField(default=datetime.now)  # 更新时间
    
    class Meta:
        table_name = 'elong_hotels'

class ElongComment(BaseModel):
    """艺龙评论模型"""
    comment_id = CharField(primary_key=True)  # 评论ID
    hotel = ForeignKeyField(ElongHotel, backref='comments')  # 关联酒店
    user_name = CharField()  # 用户名
    rating = FloatField()  # 评分
    content = TextField()  # 评论内容
    checkin_time = CharField(null=True)  # 入住时间
    room_type = CharField(null=True)  # 房间类型
    travel_type = CharField(null=True)  # 出行类型
    source = CharField(null=True)  # 评论来源
    images = TextField(null=True)  # 图片URL列表
    image_count = IntegerField(default=0)  # 图片数量
    like_count = IntegerField(default=0)  # 点赞数
    reply_content = TextField(null=True)  # 回复内容
    reply_time = DateTimeField(null=True)  # 回复时间
    comment_time = DateTimeField(null=True)  # 评论时间
    is_hidden = BooleanField(default=False)  # 是否隐藏
    created_at = DateTimeField(default=datetime.now)  # 创建时间
    updated_at = DateTimeField(default=datetime.now)  # 更新时间
    
    class Meta:
        table_name = 'elong_comments'
    
   
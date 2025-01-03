"""数据库连接管理模块"""

from peewee import MySQLDatabase, Model
from config import DB_CONFIG

# 创建数据库连接
db = MySQLDatabase(
    DB_CONFIG['database'],
    **{k: v for k, v in DB_CONFIG.items() if k != 'database'}
)

class BaseModel(Model):
    """基础模型类"""
    class Meta:
        database = db 
"""数据库包"""

from .connection import db, BaseModel
from .manager import DatabaseManager, init_database
from .models.ctrip import CtripHotel, CtripComment, CtripQA
from .models.elong import ElongHotel, ElongComment
from .models.qunar import QunarHotel, QunarComment, QunarQA

__all__ = [
    'db',
    'BaseModel',
    'DatabaseManager',
    'init_database',
    'CtripHotel',
    'CtripComment',
    'CtripQA',
    'ElongHotel',
    'ElongComment',
    'QunarHotel',
    'QunarComment',
    'QunarQA'
] 
"""数据库模型包"""

from .ctrip import CtripHotel, CtripComment, CtripQA
from .elong import ElongHotel, ElongComment
from .qunar import QunarHotel, QunarComment, QunarQA

__all__ = [
    'CtripHotel',
    'CtripComment',
    'CtripQA',
    'ElongHotel',
    'ElongComment',
    'QunarHotel',
    'QunarComment',
    'QunarQA'
] 
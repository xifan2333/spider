"""数据库管理模块"""

from .connection import db
from .models.ctrip import CtripHotel, CtripComment, CtripQA
from .models.elong import ElongHotel, ElongComment
from .models.qunar import QunarHotel, QunarComment, QunarQA
from utils.logger import setup_logger

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        """初始化数据库管理器"""
        self.db = db
        self.logger = setup_logger("database")
        self._ensure_connection()
        self._create_tables()
    
    def _ensure_connection(self):
        """确保数据库连接"""
        try:
            if not self.db.is_closed():
                self.db.close()
            self.db.connect(reuse_if_open=True)
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise
    
    def _create_tables(self):
        """创建数据表"""
        try:
            with self.db:
                # 创建携程相关表
                self.db.create_tables([CtripHotel, CtripComment, CtripQA])
                
                # 创建艺龙相关表
                self.db.create_tables([ElongHotel, ElongComment])
                
                # 创建去哪儿相关表
                self.db.create_tables([QunarHotel, QunarComment, QunarQA])
                
                self.logger.info("数据表创建成功")
        except Exception as e:
            self.logger.error(f"创建数据表失败: {str(e)}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if not self.db.is_closed():
            self.db.close()
            self.logger.info("数据库连接已关闭")

def init_database():
    """初始化数据库"""
    logger = setup_logger("database")
    try:
        manager = DatabaseManager()
        logger.info("数据库初始化成功")
        return manager
    except Exception as e:
        logger.error(f"数据库初始化失败: {str(e)}")
        raise

if __name__ == "__main__":
    # 初始化数据库
    init_database() 
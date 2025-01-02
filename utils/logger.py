import logging
import sys
from pathlib import Path
from typing import Optional

class CustomFormatter(logging.Formatter):
    """自定义日志格式化器，支持emoji"""
    
    class Emoji:
        SUCCESS = "✅"
        ERROR = "❌"
        SKIP = "⏭️"
        PROGRESS = "📊"
        HOTEL = "🏨"
        COMMENT = "💬"
        QA = "❓"
        IMAGE = "📷"
        EXCEL = "📊"
        AI = "🤖"
        COMPLETE = "🎉"
        END = "🔍"
        PROXY = "🔄"
        ACCOUNT = "👤"

    def format(self, record):
        if not hasattr(record, 'emoji'):
            if record.levelno == logging.INFO:
                record.emoji = self.Emoji.SUCCESS
            elif record.levelno == logging.ERROR:
                record.emoji = self.Emoji.ERROR
            elif record.levelno == logging.WARNING:
                record.emoji = self.Emoji.SKIP
            else:
                record.emoji = ""
        return super().format(record)

def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_dir: Optional[Path] = None
) -> logging.Logger:
    """配置logger
    
    Args:
        name: 日志记录器名称
        level: 日志级别
        log_file: 日志文件名，如果不指定则只输出到控制台
        log_dir: 日志目录，如果指定log_file则必须指定
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 如果logger已经有处理器，说明已经配置过，直接返回
    if logger.handlers:
        return logger
    
    formatter = CustomFormatter(
        '%(emoji)s %(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器
    if log_file and log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            log_dir / log_file,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger 
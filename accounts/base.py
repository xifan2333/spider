from abc import ABC, abstractmethod
from utils.logger import setup_logger, CustomFormatter
from typing import Dict, Optional
from pathlib import Path
import json
import random
from datetime import datetime

class BaseAccountPool(ABC):
    """账号池基类"""
    
    def __init__(self, platform: str):
        """初始化账号池
        
        Args:
            platform: 平台名称(qunar/ctrip/elong)
        """
        self.platform = platform
        self.accounts: Dict[str, Dict] = {}  # 使用手机号作为key
        
        # 设置日志
        self.logger = setup_logger(f"{platform}_account_pool")
        
        # 加载账号
        self._load_accounts()

    def _load_accounts(self):
        """从文件加载账号"""
        account_file = Path("accounts/accounts.json")
        if account_file.exists():
            try:
                with open(account_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    # 只加载对应平台的账号
                    self.accounts = {
                        account["phone"]: account 
                        for account in data["accounts"]
                        if account["platform"] == self.platform
                    }
                self.logger.info(
                    f"已加载 {len(self.accounts)} 个{self.platform}账号",
                    extra={"emoji": CustomFormatter.Emoji.ACCOUNT}
                )
            except Exception as e:
                self.logger.error(f"加载账号文件失败: {e}")
    
    def _save_accounts(self):
        """保存账号到文件"""
        account_file = Path("accounts/accounts.json")
        try:
            # 读取现有的所有账号
            if account_file.exists():
                with open(account_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    all_accounts = {
                        (account["phone"], account["platform"]): account 
                        for account in data["accounts"]
                    }
            else:
                all_accounts = {}
            
            # 更新当前平台的账号
            for account in self.accounts.values():
                all_accounts[(account["phone"], account["platform"])] = account
            
            # 保存所有账号
            data = {
                "accounts": list(all_accounts.values())
            }
            with open(account_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(
                f"已保存 {len(self.accounts)} 个{self.platform}账号",
                extra={"emoji": CustomFormatter.Emoji.ACCOUNT}
            )
        except Exception as e:
            self.logger.error(f"保存账号文件失败: {e}")
    
    def get_account(self) -> Optional[Dict]:
        """获取随机可用账号"""
        available = [acc for acc in self.accounts.values() if acc["is_valid"]]
        if not available:
            return None
            
        selected = random.choice(available)
        selected["last_used"] = datetime.now().isoformat()
        self._save_accounts()
        
        return {
            "phone": selected["phone"],
            "cookies": selected["cookies"]
        }
    
    def mark_account_invalid(self, phone: str):
        """标记账号为无效"""
        if phone in self.accounts:
            self.accounts[phone]["is_valid"] = False
            self.accounts[phone]["fail_count"] += 1
            self._save_accounts()
            self.logger.warning(
                f"标记{self.platform}账号为无效: {phone}",
                extra={"emoji": CustomFormatter.Emoji.ACCOUNT}
            )
    
    def get_cookies_str(self, cookies: Dict[str, str]) -> str:
        """获取cookies字符串"""
        return "; ".join([f"{k}={v}" for k, v in cookies.items()])
    
    @abstractmethod
    def verify_account(self, phone: str, cookies: Dict[str, str]) -> bool:
        """验证账号是否可用
        
        Args:
            phone: 手机号
            cookies: Cookie字典
            
        Returns:
            bool: 账号是否有效
        """
        pass 
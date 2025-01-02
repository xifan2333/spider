import requests
from ..base import BaseAccountPool

class ElongAccountPool(BaseAccountPool):
    """艺龙账号池"""
    
    def __init__(self):
        super().__init__("elong")
    
    def verify_account(self, phone: str, cookies: str) -> bool:
        """验证艺龙账号是否可用"""
        try:
            headers = {
                "Cookie": cookies,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            # 使用艺龙的用户信息API验证
            response = requests.get(
                "https://my.elong.com/Account/GetUserInfo",
                headers=headers,
                timeout=10
            )
            
            # 检查响应状态
            if response.status_code == 200:
                result = response.json()
                # 根据实际API响应结构判断登录状态
                return result.get("Success", False)
            
            return False
            
        except Exception as e:
            self.logger.error(f"验证账号失败 {phone}: {e}")
            return False


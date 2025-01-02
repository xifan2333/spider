import requests
from datetime import datetime, timedelta
from ..base import BaseAccountPool
from typing import Dict

class QunarAccountPool(BaseAccountPool):
    """去哪儿网账号池"""
    
    def __init__(self):
        super().__init__("qunar")
        
        # API配置
        self.api_url = "https://touch.qunar.com/hotelcn/api/hotellist"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json;charset=UTF-8",
            "origin": "https://touch.qunar.com",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6"
        }
    
  
    
    def verify_account(self, phone: str, cookies: Dict[str, str]) -> bool:
        """验证去哪儿网账号是否可用
        
        使用获取酒店列表接口验证，如果响应中没有ret字段或ret为False则账号无效
        """
        try:
            # 构造请求头
            headers = self.headers.copy()
            
            # 创建新的session来验证
            session = requests.Session()
            session.headers.update(headers)
            
            # 设置cookies
            for key, value in cookies.items():
                session.cookies.set(key, value)
            
            # 获取明天和后天的日期
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            day_after = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            
            # 构造请求体
            payload = {
                "city": "深圳",
                "cityUrl": "shenzhen",
                "checkInDate": tomorrow,
                "checkOutDate": day_after,
                "page": 1
            }
            
            # 发送请求
            response = session.post(
                self.api_url,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            # 解析响应
            result = response.json()
            
            # 检查ret字段
            if not result.get("ret"):
                self.logger.warning(f"账号 {phone} 验证失败: ret为False")
                return False
                
            self.logger.info(f"账号 {phone} 验证成功")
            return True
            
        except Exception as e:
            self.logger.error(f"验证账号失败 {phone}: {e}")
            return False



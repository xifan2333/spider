import requests
from typing import Dict
from datetime import datetime, timedelta
from ..base import BaseAccountPool

class CtripAccountPool(BaseAccountPool):
    """携程账号池"""
    
    def __init__(self):
        super().__init__("ctrip")
        
        # API配置
        self.api_url = "https://m.ctrip.com/restapi/soa2/31454/gethotellist"
        self.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/json",
            "locale": "zh-CN",
            "origin": "https://m.ctrip.com",
            "priority": "u=1, i"
        }
    
    def verify_account(self, phone: str, cookies: Dict[str, str]) -> bool:
        """验证携程账号是否可用
        
        使用获取酒店列表接口验证，如果响应中没有data字段则账号无效
        """
        try:
            # 构造请求头
            headers = self.headers.copy()
            headers["Cookie"] = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            
            # 获取明天和后天的日期
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            day_after = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")
            
            # 构造请求体
            payload = {
                "paging": {"pageIndex": 1, "pageSize": 1},
                "location": {
                    "countryId": 66,
                    "provinceId": 10094,
                    "districtId": 0,
                    "cityId": -1,
                    "isOversea": True,
                },
                "date": {
                    "checkInDate": tomorrow,
                    "checkOutDate": day_after
                }
            }
            
            # 发送请求
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            # 解析响应
            result = response.json()
            
            # 检查是否有data字段
            if "data" not in result:
                self.logger.warning(f"账号 {phone} 验证失败: 响应中没有data字段")
                return False
                
            self.logger.info(f"账号 {phone} 验证成功")
            return True
            
        except Exception as e:
            self.logger.error(f"验证账号失败 {phone}: {e}")
            return False
        
   


if __name__ == "__main__":
    pool = CtripAccountPool()
    account = pool.get_account()
    if account:
        print(f"获取到账号: {account['phone']}")
        is_valid = pool.verify_account(account["phone"], account["cookies"])
        print(f"账号 {account['phone']} 验证结果: {is_valid}")
        

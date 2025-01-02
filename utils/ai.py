from typing import Dict, List
import requests
from utils.logger import setup_logger

logger = setup_logger(__name__)

# AI配置
OPENAI_API_KEY = "sk-327ffc8487644e37899acd05435432f8"
OPENAI_API_URL = "https://api.deepseek.com/chat/completions"
OPENAI_MODEL = "deepseek-chat"

# AI提示词配置
PROMPTS = {
    'SYSTEM_ROLE': "你是一个专业的酒店点评分析专家。",
    
    'COMMENT_TEMPLATE': """请根据以下酒店信息和用户评论,生成一段全面的酒店点评：

酒店基本信息：
- 总评分: {总评分}
- 环境评分: {环境评分}
- 设施评分: {设施评分} 
- 服务评分: {服务评分}
- 卫生评分: {卫生评分}
- 好评率: {好评率}%

用户评论：
{评论列表}

请生成一段不超过50字的全面点评,包括位置、设施服务等方面。重点关注用户评价中提到的优点和不足。全面点评中不出现酒店名，使用'该酒店'代替。""",

    'DETAILED_COMMENT_TEMPLATE': """我学习了真实住客点评后为您总结：

酒店基本信息：
- 总评分: {总评分}
- 环境评分: {环境评分}
- 设施评分: {设施评分} 
- 服务评分: {服务评分}
- 卫生评分: {卫生评分}
- 好评率: {好评率}%

用户评论摘要：
{评论列表}

请按以下格式生成点评：
1. 首先生成一段不超过150字的总体点评
2. 然后分别从以下维度详细评价：
   - 服务: 员工服务态度、效率等
   - 位置: 地理位置优势、交通便利性等
   - 卫生: 客房和公共区域的清洁度等

注意：
- 使用"该酒店"代替具体酒店名称
- 评价要客观真实，基于实际用户评论
- 每个维度的评价控制在40字以内
- 重点突出用户反馈的特点
- 两部分内容不换行.整个文本无换行"""
}

class AIGenerator:
    """AI评论生成器"""
    
    def __init__(self):
        """初始化AI生成器"""
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }

    def _call_openai_api(self, messages: List[Dict]) -> str:
        """调用OpenAI API"""
        try:
            payload = {
                "model": OPENAI_MODEL,
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 500
            }
            
            response = requests.post(
                OPENAI_API_URL,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            return result["choices"][0]["message"]["content"]
            
        except Exception as e:
            logger.error(f"调用OpenAI API失败: {str(e)}")
            return ""

    def generate_comment(self, hotel_info: Dict, comments: List[Dict]) -> str:
        """生成AI点评"""
        try:
            # 准备评论数据
            comment_texts = [
                f"- {comment.get('评论内容', '')}"
                for comment in comments[:5]  # 只使用前5条评论
            ]
            
            # 格式化提示词
            prompt = PROMPTS["COMMENT_TEMPLATE"].format(
                总评分=hotel_info.get("总评分", "无"),
                环境评分=hotel_info.get("环境评分", "无"),
                设施评分=hotel_info.get("设施评分", "无"),
                服务评分=hotel_info.get("服务评分", "无"),
                卫生评分=hotel_info.get("卫生评分", "无"),
                好评率=hotel_info.get("好评率", "无"),
                评论列表="\n".join(comment_texts)
            )
            
            messages = [
                {"role": "system", "content": PROMPTS["SYSTEM_ROLE"]},
                {"role": "user", "content": prompt}
            ]
            
            return self._call_openai_api(messages)
            
        except Exception as e:
            logger.error(f"生成AI点评失败: {str(e)}")
            return ""

    def generate_detailed_comment(self, hotel_info: Dict, comments: List[Dict]) -> str:
        """生成AI详细点评"""
        try:
            # 准备评论数据
            comment_texts = [
                f"- {comment.get('评论内容', '')}"
                for comment in comments[:10]  # 使用前10条评论
            ]
            
            # 格式化提示词
            prompt = PROMPTS["DETAILED_COMMENT_TEMPLATE"].format(
                总评分=hotel_info.get("总评分", "无"),
                环境评分=hotel_info.get("环境评分", "无"),
                设施评分=hotel_info.get("设施评分", "无"),
                服务评分=hotel_info.get("服务评分", "无"),
                卫生评分=hotel_info.get("卫生评分", "无"),
                好评率=hotel_info.get("好评率", "无"),
                评论列表="\n".join(comment_texts)
            )
            
            messages = [
                {"role": "system", "content": PROMPTS["SYSTEM_ROLE"]},
                {"role": "user", "content": prompt}
            ]
            
            return self._call_openai_api(messages)
            
        except Exception as e:
            logger.error(f"生成AI详细点评失败: {str(e)}")
            return "" 
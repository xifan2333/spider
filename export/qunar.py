import pandas as pd
from typing import List, Dict
from datetime import datetime
from db.models.qunar import QunarHotel, QunarComment, QunarQA
from utils.logger import setup_logger

logger = setup_logger(__name__)

class QunarExporter:
    """去哪儿数据导出器"""
    
    def __init__(self):
        self.excel_path = f"data/qunar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def export_hotels(self) -> List[Dict]:
        """导出酒店数据"""
        try:
            hotels = []
            for hotel in QunarHotel.select():
                hotels.append({
                    '酒店ID': hotel.hotel_id,
                    '酒店名称': hotel.name,
                    '英文名称': hotel.en_name,
                    '纬度': hotel.latitude,
                    '经度': hotel.longitude,
                    '星级': hotel.level,
                    '评分': hotel.score,
                    '地址': hotel.address,
                    '电话': hotel.phone,
                    '评论数': hotel.comment_count,
                    '一句话亮点': hotel.highlight,
                    '开业时间': hotel.open_time,
                    '装修时间': hotel.fitment_time,
                    '房间数': hotel.room_count,
                    '评论标签': hotel.comment_tags,
                    '榜单': hotel.ranking,
                    '好评率': hotel.good_rate,
                    '地点优势': hotel.location_advantage,
                    '设施服务': hotel.facilities,
                    '交通信息': hotel.traffic_info,
                    '详细评分': hotel.detail_score,
                    '平台优选': hotel.is_platform_choice,
                    'AI点评': hotel.ai_comment,
                    'AI图片': hotel.ai_images,
                    '创建时间': hotel.created_at,
                    '更新时间': hotel.updated_at
                })
            return hotels
        except Exception as e:
            logger.error(f"导出酒店数据失败: {str(e)}")
            return []

    def export_comments(self) -> List[Dict]:
        """导出评论数据"""
        try:
            comments = []
            for comment in QunarComment.select():
                comments.append({
                    '评论ID': comment.comment_id,
                    '酒店ID': comment.hotel.hotel_id,
                    '用户名': comment.username,
                    '评分': comment.score,
                    '内容': comment.content,
                    '入住时间': comment.check_in_date,
                    '房型': comment.room_type,
                    '出行类型': comment.trip_type,
                    '来源': comment.source,
                    'IP地址': comment.ip_location,
                    '图片': comment.images,
                    '图片数量': comment.image_count,
                    '点赞数': comment.like_count,
                    '回复内容': comment.reply_content,
                    '回复时间': comment.reply_time,
                    '评论时间': comment.comment_time,
                    '创建时间': comment.created_at
                })
            return comments
        except Exception as e:
            logger.error(f"导出评论数据失败: {str(e)}")
            return []

    def export_qas(self) -> List[Dict]:
        """导出问答数据"""
        try:
            qas = []
            for qa in QunarQA.select():
                qas.append({
                    '问答ID': qa.qa_id,
                    '酒店ID': qa.hotel.hotel_id,
                    '问题': qa.question,
                    '提问者昵称': qa.asker_nickname,
                    '提问时间': qa.ask_time,
                    '回答数': qa.answer_count,
                    '问题来源': qa.question_source,
                    '回答ID': qa.answer_id,
                    '回答者昵称': qa.answerer_nickname,
                    '回答时间': qa.answer_time,
                    '回答内容': qa.answer_content,
                    '是否官方回答': qa.is_official,
                    '创建时间': qa.created_at
                })
            return qas
        except Exception as e:
            logger.error(f"导出问答数据失败: {str(e)}")
            return []

    def export_to_excel(self):
        """导出所有数据到Excel"""
        try:
            # 创建Excel写入器
            writer = pd.ExcelWriter(self.excel_path, engine='openpyxl')
            
            # 导出酒店数据
            hotels = self.export_hotels()
            if hotels:
                df_hotels = pd.DataFrame(hotels)
                df_hotels.to_excel(writer, sheet_name='酒店', index=False)
                logger.info(f"导出酒店数据成功: {len(hotels)}条")
            
            # 导出评论数据
            comments = self.export_comments()
            if comments:
                df_comments = pd.DataFrame(comments)
                df_comments.to_excel(writer, sheet_name='评论', index=False)
                logger.info(f"导出评论数据成功: {len(comments)}条")
            
            # 导出问答数据
            qas = self.export_qas()
            if qas:
                df_qas = pd.DataFrame(qas)
                df_qas.to_excel(writer, sheet_name='问答', index=False)
                logger.info(f"导出问答数据成功: {len(qas)}条")
            
            # 保存Excel文件
            writer.close()
            logger.info(f"导出Excel文件成功: {self.excel_path}")
            
        except Exception as e:
            logger.error(f"导出Excel失败: {str(e)}")

def main():
    """主函数"""
    exporter = QunarExporter()
    exporter.export_to_excel()

if __name__ == "__main__":
    main()

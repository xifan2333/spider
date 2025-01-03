import pandas as pd
from typing import List, Dict
from datetime import datetime
from db.models.ctrip import CtripHotel, CtripComment, CtripQA
from utils.logger import setup_logger

logger = setup_logger(__name__)

class CtripExporter:
    """携程数据导出器"""
    
    def __init__(self):
        self.excel_path = f"data/ctrip_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def export_hotels(self) -> List[Dict]:
        """导出酒店数据"""
        try:
            hotels = []
            for hotel in CtripHotel.select():
                hotels.append({
                    '酒店ID': hotel.hotel_id,
                    '酒店名称': hotel.name,
                    '英文名称': hotel.name_en,
                    '地址': hotel.address,
                    '位置描述': hotel.location_desc,
                    '经度': hotel.longitude,
                    '纬度': hotel.latitude,
                    '星级': hotel.star,
                    '标签': hotel.tags,
                    '一句话点评': hotel.one_sentence_comment,
                    'AI点评': hotel.ai_comment,
                    'AI详细点评': hotel.ai_detailed_comment,
                    '总评分': hotel.rating_all,
                    '位置评分': hotel.rating_location,
                    '设施评分': hotel.rating_facility,
                    '服务评分': hotel.rating_service,
                    '房间评分': hotel.rating_room,
                    '评论数': hotel.comment_count,
                    '评论标签': hotel.comment_tags,
                    '好评数': hotel.good_comment_count,
                    '差评数': hotel.bad_comment_count,
                    '好评率': hotel.good_rate,
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
            for comment in CtripComment.select():
                comments.append({
                    '评论ID': comment.comment_id,
                    '酒店ID': comment.hotel.hotel_id,
                    '用户名': comment.user_name,
                    '用户等级': comment.user_level,
                    '用户身份': comment.user_identity,
                    '评分': comment.rating,
                    '内容': comment.content,
                    '入住时间': comment.checkin_time,
                    '房型': comment.room_type,
                    '出行类型': comment.travel_type,
                    '来源': comment.source,
                    '有用数': comment.useful_count,
                    'IP位置': comment.ip_location,
                    '图片': comment.images,
                    '酒店回复': comment.hotel_reply,
                    '回复时间': comment.reply_time,
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
            for qa in CtripQA.select():
                qas.append({
                    '问答ID': qa.qa_id,
                    '酒店ID': qa.hotel.hotel_id,
                    '问题': qa.question,
                    '提问时间': qa.ask_time,
                    '提问者': qa.asker,
                    '回复数': qa.reply_count,
                    '回复内容': qa.replies,
                    '创建时间': qa.created_at
                })
            return qas
        except Exception as e:
            logger.error(f"导出问答数据失败: {str(e)}")
            return []

    def export_to_excel(self):
        """导出所有数据到Excel"""
        try:
            writer = pd.ExcelWriter(self.excel_path, engine='openpyxl')
            
            hotels = self.export_hotels()
            if hotels:
                df_hotels = pd.DataFrame(hotels)
                df_hotels.to_excel(writer, sheet_name='酒店', index=False)
                logger.info(f"导出酒店数据成功: {len(hotels)}条")
            
            comments = self.export_comments()
            if comments:
                df_comments = pd.DataFrame(comments)
                df_comments.to_excel(writer, sheet_name='评论', index=False)
                logger.info(f"导出评论数据成功: {len(comments)}条")
            
            qas = self.export_qas()
            if qas:
                df_qas = pd.DataFrame(qas)
                df_qas.to_excel(writer, sheet_name='问答', index=False)
                logger.info(f"导出问答数据成功: {len(qas)}条")
            
            writer.close()
            logger.info(f"导出Excel文件成功: {self.excel_path}")
            
        except Exception as e:
            logger.error(f"导出Excel失败: {str(e)}")

def main():
    """主函数"""
    exporter = CtripExporter()
    exporter.export_to_excel()

if __name__ == "__main__":
    main() 
import pandas as pd
from typing import List, Dict
from datetime import datetime
from db.models.elong import ElongHotel, ElongComment
from utils.logger import setup_logger

logger = setup_logger(__name__)

class ElongExporter:
    """艺龙数据导出器"""
    
    def __init__(self):
        self.excel_path = f"data/elong_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
    def export_hotels(self) -> List[Dict]:
        """导出酒店数据"""
        try:
            hotels = []
            for hotel in ElongHotel.select():
                hotels.append({
                    '酒店ID': hotel.hotel_id,
                    '酒店名称': hotel.name,
                    '英文名称': hotel.name_en,
                    '地址': hotel.address,
                    '位置信息': hotel.location,
                    '星级': hotel.star,
                    '标签': hotel.tags,
                    '主要标签': hotel.main_tag,
                    '交通信息': hotel.traffic_info,
                    '城市名称': hotel.city_name,
                    '总评分': hotel.score,
                    '服务评分': hotel.score_service,
                    '位置评分': hotel.score_location,
                    '设施评分': hotel.score_facility,
                    '卫生评分': hotel.score_hygiene,
                    '性价比评分': hotel.score_cost,
                    '评分描述': hotel.score_desc,
                    '评论数': hotel.comment_count,
                    '好评率': hotel.good_rate,
                    '好评数': hotel.good_count,
                    '差评数': hotel.bad_count,
                    '图片数量': hotel.image_count,
                    '视频数量': hotel.video_count,
                    'AI评论总结': hotel.ai_summary,
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
            for comment in ElongComment.select():
                comments.append({
                    '评论ID': comment.comment_id,
                    '酒店ID': comment.hotel.hotel_id,
                    '用户名': comment.user_name,
                    '评分': comment.rating,
                    '内容': comment.content,
                    '入住时间': comment.checkin_time,
                    '房型': comment.room_type,
                    '出行类型': comment.travel_type,
                    '来源': comment.source,
                    '图片': comment.images,
                    '图片数量': comment.image_count,
                    '点赞数': comment.like_count,
                    '回复内容': comment.reply_content,
                    '回复时间': comment.reply_time,
                    '评论时间': comment.comment_time,
                    '是否隐藏': comment.is_hidden,
                    '创建时间': comment.created_at
                })
            return comments
        except Exception as e:
            logger.error(f"导出评论数据失败: {str(e)}")
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
            
            writer.close()
            logger.info(f"导出Excel文件成功: {self.excel_path}")
            
        except Exception as e:
            logger.error(f"导出Excel失败: {str(e)}")

def main():
    """主函数"""
    exporter = ElongExporter()
    exporter.export_to_excel()

if __name__ == "__main__":
    main() 
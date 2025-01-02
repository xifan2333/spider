# -*- coding: utf-8 -*-
import requests
from fake_useragent import UserAgent
from typing import Dict, List
import time
import json
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
from peewee import MySQLDatabase, Model, CharField, TextField, IntegerField, FloatField, DateTimeField, ForeignKeyField

# ================ 配置部分 ================

# 数据库配置
DB_CONFIG = {
    'host': '10.0.0.253',
    'port': 3306,
    'user': 'root',
    'password': '583379',
    'database': 'hotel-spider'
}

# 创建数据库连接
db = MySQLDatabase(
    DB_CONFIG['database'],
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    charset='utf8mb4'
)

# 定义数据库模型
class BaseModel(Model):
    class Meta:
        database = db

class ElongHotel(BaseModel):
    hotel_id = CharField(primary_key=True)
    name = CharField()
    name_en = CharField(null=True)
    address = TextField(null=True)
    location = TextField(null=True)
    star = CharField(null=True)
    tags = TextField(null=True)
    main_tag = TextField(null=True)
    traffic_info = TextField(null=True)
    rating_all = FloatField(null=True)
    rating_location = FloatField(null=True)
    rating_facility = FloatField(null=True)
    rating_service = FloatField(null=True)
    rating_hygiene = FloatField(null=True)
    rating_cost = FloatField(null=True)
    rating_desc = TextField(null=True)
    comment_count = IntegerField(null=True)
    city_name = CharField(null=True)
    created_at = DateTimeField(default=datetime.now)
    updated_at = DateTimeField(default=datetime.now)

class ElongComment(BaseModel):
    comment_id = CharField(primary_key=True)
    hotel = ForeignKeyField(ElongHotel, backref='comments')
    user_name = CharField()
    user_level = IntegerField(null=True)
    rating = FloatField()
    content = TextField()
    comment_time = DateTimeField()
    useful_count = IntegerField(default=0)
    travel_type = CharField(null=True)
    room_type = CharField(null=True)
    checkin_time = DateTimeField(null=True)
    source = CharField(null=True)
    ip_location = CharField(null=True)
    images = TextField(null=True)
    local_images = TextField(null=True)
    hotel_reply = TextField(null=True)
    reply_time = DateTimeField(null=True)
    created_at = DateTimeField(default=datetime.now)

# 创建数据表
def create_tables():
    """创建数据表"""
    try:
        with db:
            db.create_tables([ElongHotel, ElongComment])
    except Exception as e:
        print(f"创建数据表失败: {str(e)}")
        raise

# 时间配置
TODAY = datetime.now().strftime("%Y-%m-%d")

# 文件路径配置
DATA_DIR = Path('data')
IMAGES_DIR = DATA_DIR / "elong_images" / TODAY
PROGRESS_FILE = DATA_DIR / f"elong-progress-{TODAY}.json"
INTERRUPT_FILE = DATA_DIR / "interrupt.flag"

# API配置
BASE_URL = "https://m.elong.com/tapi/v2/list"
COMMENT_URL = "https://m.elong.com/commonpage/getCommentList"
SCORE_URL = "https://m.elong.com/commonpage/getCommentInfo"  # 添加评分接口URL

# 请求头配置
BASE_HEADERS = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en-GB;q=0.7,en;q=0.6",
    "appfrom": "13",
    "cluster": "idc",
    "deviceid": "300ef766-699f-4eb9-aec4-0a1f67035d55",
    "h-env": "",
    "h-os": "",
    "priority": "u=1, i",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Microsoft Edge";v="132"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "timezone": "8",
    "tmapi-client": "i-eh5",
}

COMMENT_HEADERS = {
    **BASE_HEADERS,
    "content-type": "application/json",
    "origin": "https://m.elong.com",
}

# 日志表情配置
EMOJI = {
    'SUCCESS': "✅",
    'ERROR': "🚫",
    'SKIP': "🦘",
    'PROGRESS': "📊",
    'CONTINUE': "📣",
    'END': "🔍",
    'COMPLETE': "🎉",
    'HOTEL': "🏨",
    'COMMENT': "💬",
    'SCORE': "🌟",
    'IMAGE': "📷",
    'EXCEL': "📊",
}

# 城市配置
CITIES = [
    {"zhname": "休斯顿", "enname": "Houston", "code": "110076723"},
    {"zhname": "奥斯汀", "enname": "Austin", "code": "110076547"},
    {"zhname": "达拉斯", "enname": "Dallas", "code": "110076315"},
    {"zhname": "圣安东尼奥", "enname": "San Antonio", "code": "110076839"},
    {"zhname": "沃思堡", "enname": "Fort Worth", "code": "110076385"},
    {"zhname": "埃尔帕索", "enname": "El Paso", "code": "110077028"},
    {"zhname": "阿灵顿", "enname": "Arlington", "code": "110075977"},
    {"zhname": "科珀斯克里斯蒂", "enname": "Corpus Christi", "code": "110076747"},
    {"zhname": "拉伯克", "enname": "Lubbock", "code": "110077084"},
    {"zhname": "加尔维斯顿", "enname": "Galveston", "code": "110076765"},
]

# 爬虫配置
PAGE_SIZE = 10
COMMENT_PAGE_SIZE = 10
REQUEST_DELAY = 0.5  # 请求延迟(秒)
COMMENT_REQUEST_DELAY = 0.2  # 评论请求延迟(秒)

ua = UserAgent(platforms="mobile")

# 添加中断处理相关的常量
INTERRUPT_FILE = DATA_DIR / "interrupt.flag"

class CustomFormatter(logging.Formatter):
    """自定义日志格式化器，支持emoji"""
    
    def format(self, record):
        if not hasattr(record, 'emoji'):
            if record.levelno == logging.INFO:
                record.emoji = EMOJI['SUCCESS']
            elif record.levelno == logging.ERROR:
                record.emoji = EMOJI['ERROR']
            elif record.levelno == logging.WARNING:
                record.emoji = EMOJI['SKIP']
            else:
                record.emoji = ""
        
        return super().format(record)

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """配置logger"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    formatter = CustomFormatter(
        '%(emoji)s %(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


class ElongCrawler:
    def __init__(self):
        # 首先设置数据目录
        self.data_dir = DATA_DIR
        self.data_dir.mkdir(exist_ok=True)
        
        # 设置logger
        self.logger = setup_logger("elong_crawler")
        
        # 初始化session和headers
        self.session = requests.Session()
        self.base_url = BASE_URL
        self.headers = {**BASE_HEADERS, "user-agent": ua.random}
        self.session.headers.update(self.headers)

        # 设置文件路径
        self.progress_file = PROGRESS_FILE
        self.images_dir = IMAGES_DIR
        self.images_dir.mkdir(exist_ok=True)

        # 添加中断标志
        self.is_interrupted = False
        # 删除可能存在的中断标志文件
        if INTERRUPT_FILE.exists():
            INTERRUPT_FILE.unlink()

        # 初始化数据库连接
        try:
            db.connect(reuse_if_open=True)
            create_tables()
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise

        # 加载进度
        self.progress = self._load_progress()
        # 添加内存缓存，减少文件IO
        self.processed_hotels = set(self.progress['processed_hotel_ids'])
        self.processed_cities = set(self.progress['processed_cities'])
        self.current_city_index = self.progress['current_city_index']
        # 添加计数器，用于定期保存进度
        self.save_counter = 0
        self.SAVE_INTERVAL = 10  # 每处理10个酒店保存一次进度

    def save_data(self, hotels: List[Dict] = None, comments: List[Dict] = None):
        """保存数据到数据库"""
        try:
            with db.atomic():
                # 先保存酒店数据
                if hotels:
                    for hotel in hotels:
                        self.save_hotel_data(hotel)
                
                # 再保存评论数据
                if comments:
                    # 确保对应的酒店已经存在
                    hotel_id = comments[0]['酒店ID'] if comments else None
                    if hotel_id:
                        try:
                            # 检查酒店是否存在
                            ElongHotel.get_by_id(hotel_id)
                            self.save_comment_data(comments)
                        except ElongHotel.DoesNotExist:
                            self.logger.error(f"保存评论失败: 酒店ID {hotel_id} 不存在")
                    else:
                        self.logger.error("保存评论失败: 无效的酒店ID")
        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            raise

    def should_update_comments(self, hotel_id: str, current_count: int) -> bool:
        """
        判断是否需要更新评论 - 由于不再加载历史数据,总是返回True
        """
        return True

    def get_hotel_list(
        self,
        city_code: str,
        check_in: str,
        check_out: str,
        page: int = 0,
        page_size: int = 10,
    ) -> Dict:
        """
        获取酒店列表
        :param city_code: 城市代码
        :param check_in: 入住日期 (格式: YYYY-MM-DD)
        :param check_out: 退房日期 (格式: YYYY-MM-DD)
        :param page: 页码，从0开始
        :param page_size: 每页数量
        :return: 响应数据的字典
        """
        params = {
            # "scriptVersion": "2.4.99",
            "city": city_code,
            # "filterList": "8888_1",
            "inDate": check_in,
            "outDate": check_out,
            "pageIndex": page,
            "pageSize": page_size,
            # "diyIsClosed": "false",
            # "_timer": str(int(time.time() * 1000)),
        }

        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            result = response.json()
            with open("hotel.json", "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            return result
        except Exception as e:
            self.logger.error("请求失败", e)
            return {}

    def parse_hotel_info(self, hotel_data: Dict) -> Dict:
        """解析酒店基本信息"""
        # 提取酒店标签
        hotel_tags = []
        if hotel_data.get("hotelTags"):
            hotel_tags = [tag["tagName"] for tag in hotel_data["hotelTags"]]

        # 构建酒店信息字典
        hotel_info = {
            "酒店ID": hotel_data.get("hotelId", ""),
            "酒店名称": hotel_data.get("hotelName", ""),
            "酒店英文名称": hotel_data.get("hotelNameEn", ""),
            "酒店标签": ",".join(hotel_tags),
            "评论主标签": hotel_data.get("commentMainTag", ""),
            "地址": hotel_data.get("hotelAddress", ""),
            "地理位置": hotel_data.get("areaName", ""),
            "酒店等级": hotel_data.get("starLevelDes", ""),
            "城市名称": hotel_data.get("cityName", ""),
            "交通信息": hotel_data.get("trafficInfo", ""),
        }

        return hotel_info

    def search_city_hotels(self, city_info: Dict, check_in: str, check_out: str, city_index: int) -> List[Dict]:
        """搜索指定城市的所有酒店，并获取评分和评论信息"""
        hotels = []
        page = 0
        total_hotels = 0

        try:
            if self._is_city_processed(city_info['code']):
                self.logger.info(
                    f"{EMOJI['SKIP']} 城市 {city_info['zhname']} 已处理，跳过"
                )
                return hotels

            # 获取总酒店数
            first_result = self.get_hotel_list(city_info["code"], check_in, check_out, page)
            if first_result and "data" in first_result:
                total_hotels = first_result["data"].get("hotelCount", 0)
                self.logger.info(
                    f"{EMOJI['CONTINUE']} 城市 {city_info['zhname']} 共有 {total_hotels} 家酒店"
                )
                # 更新进度信息
                self.progress['last_city_total'] = total_hotels
                self.progress['last_city_processed'] = 0

            # 遍历所有页面
            while True:
                if self._check_interrupt():
                    break
                    
                self.logger.info(f"正在获取第 {page + 1} 页酒店列表")
                result = self.get_hotel_list(city_info["code"], check_in, check_out, page)

                if not result or "data" not in result or not result["data"].get("hotelList"):
                    break

                for hotel_data in result["data"]["hotelList"]:
                    if self._check_interrupt():
                        break
                        
                    hotel_info = self.parse_hotel_info(hotel_data)
                    hotel_id = str(hotel_info["酒店ID"])

                    if self._is_hotel_processed(hotel_id):
                        self.logger.info(
                            f"{EMOJI['SKIP']} 酒店 {hotel_info['酒店名称']} 已处理，跳过"
                        )
                        continue

                    try:
                        # 获取评分信息
                        score_info = self.get_hotel_score_info(hotel_id)
                        if score_info:
                            hotel_info.update(score_info)
                        
                        self.log_hotel_info(hotel_info)

                        # 先保存酒店信息
                        self.save_data(hotels=[hotel_info])
                        hotels.append(hotel_info)

                        # 再获取和保存评论信息
                        comments = self.get_all_hotel_comments(
                            city_index=city_index,
                            hotel_id=hotel_id,
                            hotel_name=hotel_info["酒店名称"],
                            hotel_en_name=hotel_info["酒店英文名称"],
                        )
                        if comments:
                            self.save_data(comments=comments)

                    except Exception as e:
                        self.logger.error(f"处理酒店 {hotel_info['酒店名称']} 的详细信息时出错: {str(e)}")
                        continue
                    
                    self.progress['last_city_processed'] += 1
                    self.log_progress(self.progress['last_city_processed'], total_hotels, "酒店采集")
                    
                    # 保存进度
                    self._save_progress(city_index, hotel_id)
                    
                    time.sleep(REQUEST_DELAY)

                if (page + 1) * PAGE_SIZE >= total_hotels or self._check_interrupt():
                    break

                page += 1
                time.sleep(REQUEST_DELAY)

        except Exception as e:
            self.logger.error(f"处理城市 {city_info['zhname']} 时出错: {str(e)}")
        finally:
            if hotels:
                self.logger.info(
                    f"{EMOJI['COMPLETE']} 完成城市 {city_info['zhname']} 的爬取，共获取 {len(hotels)} 家酒店"
                )
                # 将城市标记为已处理
                if self.progress['last_city_processed'] >= self.progress['last_city_total']:
                    self.processed_cities.add(city_info['code'])
                # 完成城市爬取后，强制保存进度
                self._save_progress(city_index + 1, force=True)
            return hotels

    def get_hotel_comments(self, hotel_id: str, page: int = 0, page_size: int = 10) -> Dict:
        """获取酒店评论"""
        params = {"scriptVersion": "0.0.33"}

        data = {
            "objectId": hotel_id,
            "keyword": "",
            "sortingInfo": {"sortingMethod": 0, "sortingDirection": 1},
            "searchFeatures": [],
            "useless": 0,
            "can_sale_ota_category_ids": ["11","6043","6020","13","6033","105","18","75","6095"],
            "pageIndex": page,
            "pageSize": page_size,
            "_timer": str(int(time.time() * 1000)),
        }

        try:
            self.session.headers.update(COMMENT_HEADERS)
            response = self.session.post(COMMENT_URL, params=params, json=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"获取评论失败: {str(e)}")
            return {}

    def convert_timestamp(self, timestamp: str) -> str:
        """
        将时间戳转换为标准格式
        :param timestamp: 毫秒级时间戳字符串
        :return: YYYY-MM-DD HH:MM:SS 格式的时间字符串
        """
        try:
            # 字符串转换为整数
            ts = int(timestamp)
            # 如果是毫秒级时间戳，转换为秒级
            if len(str(ts)) > 10:
                ts = ts / 1000
            # 转换为datetime对象
            dt = datetime.fromtimestamp(ts)
            # 格式化输出
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return ""

    def convert_iso_time(self, iso_time: str) -> str:
        """
        ISO格式时间转换为标准格式
        :param iso_time: ISO格式的时间字符串
        :return: YYYY-MM-DD HH:MM:SS 格式的时间字符串
        """
        try:
            # 解析ISO格式时间字符串
            dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
            # 转换为本地时间
            local_dt = dt.astimezone()
            # 格式化输出
            return local_dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return ""

    def get_comment_source(self, source: int, real_source: int) -> str:
        """
        获取评论来源
        :param source: 来源代码
        :param real_source: 真实来源代码
        :return: 评论来源描述
        """
        if real_source == 63:
            return "Expedia"
        elif real_source == 64:
            return "Hotels.com"
        elif source == 60:  # 国际版
            return "International"
        return "艺龙"

    def download_comment_images(
        self,
        image_urls: List[str],
        local_image_names: List[str]
    ) -> List[str]:
        """
        串行下载评论图片
        """
        if not image_urls:
            return []

        save_dir = self.images_dir
        downloaded_images = []

        for index, (url, file_name) in enumerate(zip(image_urls, local_image_names)):
            try:
                if self._check_interrupt():
                    break
                    
                # 下载图片
                response = self.session.get(url, stream=True)
                response.raise_for_status()
                
                file_path = save_dir / file_name
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                self.logger.info(
                    f"{EMOJI['IMAGE']} 下载评论图片成功: {file_name}"
                )
                downloaded_images.append(file_name)
                    
                # 添加延时,避免请求过快
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"下载图片失败: {url}, 错误: {str(e)}")
                continue

        return downloaded_images

    def clean_text(self, text: str) -> str:
        """
        清理文本内容
        :param text: 原始文本
        :return: 清理后的文本
        """
        if not text:
            return ""
        # 替换换行符为空格
        text = text.replace("\n", " ").replace("\r", " ")
        # 替换多个空格为单个空格
        text = " ".join(text.split())
        # 去除首尾空白字符
        return text.strip()

    def parse_comment_info(
        self,
        comment_data: Dict,
        hotel_id: str,
        hotel_name: str = "",
        hotel_en_name: str = "",
    ) -> Dict:
        """
        解析评论信息
        """
        try:
            # 获取用户信息
            user_info = comment_data.get("commentUser", {})

            # 获取订单信息
            comment_ext = comment_data.get("commentExt", {})
            order_info = comment_ext.get("order", {})

            # 处理评论图片
            image_urls = []
            local_image_names = []  # 存储本地图片文件名
            if comment_data.get("images"):
                for img in comment_data["images"]:
                    image_paths = img.get("imagePaths", [])
                    for path in image_paths:
                        if path.get("specId") == 403:  # 使用480_320尺寸的图片
                            url = path.get("url", "")
                            if url:
                                image_urls.append(url)
                                # 生成高清图URL
                                quality = url.split("/")[-2]
                                name = url.split("/")[-1]
                                hd_name = "nw_" + name
                                hd_url = url.replace(quality, "minsu_540*1500").replace(
                                    name, hd_name
                                )
                                image_urls[-1] = hd_url  # 替换为高清图URL

            # 生成本地图片文件名并下载图片
            if hotel_name and image_urls:
                # 清理酒店名和用户名中的非法字符
                safe_hotel_name = hotel_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
                user_name = self.clean_text(user_info.get("nickName", "匿名用户"))
                safe_user_name = user_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
                comment_id = comment_data.get("commentId", "")
                
                # 如果是匿名用户或合作平台用户，添加评论ID以区分
                if user_name in ["匿名用户", ""] or "平台用户" in user_name:
                    safe_user_name = f"{safe_user_name}_{comment_id}"
                
                for index, url in enumerate(image_urls, 1):
                    file_name = f"{safe_hotel_name}_{safe_user_name}_{index:02d}.jpg"
                    local_image_names.append(file_name)

                # 下载图片
                self.download_comment_images(
                    image_urls=image_urls,
                    local_image_names=local_image_names
                )

            # 转换时间格式
            comment_time = self.convert_iso_time(comment_data.get("createTime", ""))
            check_in_time = self.convert_timestamp(order_info.get("checkInTime", ""))

            # 获取评论来源
            source = comment_data.get("source", 0)
            real_source = comment_data.get("realSource", 0)
            comment_source = self.get_comment_source(source, real_source)

            # 处理酒店回复
            reply = ""
            reply_time = ""
            if comment_data.get("replys"):
                first_reply = comment_data["replys"][0] if comment_data["replys"] else {}
                reply = self.clean_text(first_reply.get("content", ""))
                if first_reply.get("createTime"):
                    reply_time = self.convert_iso_time(first_reply["createTime"])

            comment_info = {
                "酒店ID": hotel_id,
                "酒店名称": self.clean_text(hotel_name),
                "酒店英文名称": self.clean_text(hotel_en_name),
                "评论ID": comment_data.get("commentId", ""),
                "用户名": self.clean_text(user_info.get("nickName", "")),
                "用户等级": user_info.get("rank", 0),
                "评分": comment_data.get("commentScore", 0),
                "评论内容": self.clean_text(comment_data.get("content", "")),
                "评论时间": comment_time,
                "有用数": comment_data.get("usefulCount", 0),
                "出行类型": self.clean_text(comment_ext.get("travelTypeDesc", "")),
                "房型": self.clean_text(order_info.get("roomTypeName", "")),
                "入住时间": check_in_time,
                "酒店回复": reply,
                "回复时间": reply_time,
                "评论图片": ",".join(image_urls),  # 存储高清图片URL
                "本地图片": ",".join(local_image_names),  # 存储本地图片文件名
                "IP归属地": self.clean_text(comment_data.get("ipAddress", "")),
                "评论来源": comment_source,
            }

            return comment_info

        except Exception as e:
            self.logger.error(f"解析评论信息失败: {str(e)}")
            return {}

    def get_all_hotel_comments(
        self,
        city_index: int,
        hotel_id: str,
        hotel_name: str = "",
        hotel_en_name: str = "",
    ) -> List[Dict]:
        """获取酒店的所有评论"""
        all_comments = []
        
        try:
            # 1. 获取第一页评论，确定总数
            first_result = self.get_hotel_comments(hotel_id, 0, COMMENT_PAGE_SIZE)
            if not first_result or "data" not in first_result:
                return all_comments

            total = first_result["data"].get("total", 0)
            if total == 0:
                self.logger.info(f"酒店 {hotel_name} 暂无评论")
                return all_comments

            total_pages = (total + COMMENT_PAGE_SIZE - 1) // COMMENT_PAGE_SIZE
            self.logger.info(f"酒店 {hotel_name} 共有 {total} 条评论，{total_pages} 页")

            # 2. 遍历所有页面
            for page in range(total_pages):
                if self._check_interrupt():
                    break

                try:
                    # 获取当前页评论
                    if page > 0:
                        self.logger.info(f"正在获取第 {page + 1}/{total_pages} 页评论")
                        result = self.get_hotel_comments(hotel_id, page, COMMENT_PAGE_SIZE)
                        if not result or "data" not in result:
                            continue
                    else:
                        result = first_result

                    # 3. 处理当前页评论
                    comments = result["data"].get("comments", [])
                    if not comments:
                        continue

                    # 4. 解析评论数据
                    page_comments = []
                    for comment_data in comments:
                        comment_info = self.parse_comment_info(
                            comment_data,
                            hotel_id=hotel_id,
                            hotel_name=hotel_name,
                            hotel_en_name=hotel_en_name
                        )
                        if comment_info:
                            page_comments.append(comment_info)

                    # 5. 添加到总评论列表
                    if page_comments:
                        all_comments.extend(page_comments)
                        self.logger.info(
                            f"获取第 {page + 1} 页评论成功: {len(page_comments)} 条",
                            extra={'emoji': EMOJI['SUCCESS']}
                        )

                    time.sleep(COMMENT_REQUEST_DELAY)

                except Exception as e:
                    self.logger.error(f"处理第 {page + 1} 页评论失败: {str(e)}")
                    continue

            # 6. 保存所有评论
            if all_comments:
                self.save_data(comments=all_comments)
                self.logger.info(
                    f"保存所有评论成功: {len(all_comments)}/{total} 条",
                    extra={'emoji': EMOJI['SUCCESS']}
                )

        except Exception as e:
            self.logger.error(f"获取酒店 {hotel_name} 的评论失败: {str(e)}")
        
        return all_comments

    def get_hotel_score_info(self, hotel_id: str) -> Dict:
        """
        获取酒店评分详细信息
        :param hotel_id: 酒店ID
        :return: 酒店评分信息字典
        """
        try:
            # 获取评分数据
            params = {
                "scriptVersion": "0.0.33",
                "hotelId": hotel_id,
                "can_sale_ota_category_ids": "11,6043,6020,13,6033,105,18,75,6095",
                "_timer": str(int(time.time() * 1000))
            }
            
            # 设置请求头
            self.session.headers.update(COMMENT_HEADERS)
            
            # 发送请求并获取响应
            response = self.session.get(SCORE_URL, params=params)
            response.raise_for_status()
            score_result = response.json()
            
            if not score_result:
                self.logger.error("获取酒店评分失败: 响应为空")
                return {}
                
            if "data" not in score_result:
                self.logger.error("获取酒店评分失败: 响应中无data字段")
                return {}

            data = score_result.get("data", {})
            if not data:
                self.logger.error("获取酒店评分失败: data为空")
                return {}

            # 构建评分信息字典
            score_info = {
                "总评分": data.get("score", 0),
                "位置评分": data.get("positionScore", 0),
                "设施评分": data.get("facilityScore", 0),
                "服务评分": data.get("serviceScore", 0),
                "卫生评分": data.get("sanitationScore", 0),
                "性价比评分": data.get("costScore", 0),
                "评分描述": data.get("commentDes", ""),
                "总评论数": data.get("commentCount", 0),
                "好评率": round(data.get("goodRate", 0) * 100, 1),
                "好评数": data.get("goodCount", 0),
                "差评数": data.get("badCount", 0),
                "AI评论": "",  # 默认为空字符串
            }

            # 安全地获取AI评论
            ai_summary = data.get("aiSummary")
            if ai_summary and isinstance(ai_summary, dict):
                score_info["AI评论"] = ai_summary.get("aiSummaryContent", "")

            return score_info

        except Exception as e:
            self.logger.error(f"获取酒店评分信息失败: {str(e)}")
            return {}

    def extract_comment_tags(self, comment_data: Dict) -> str:
        """
        从评论数据中提取标签
        :param comment_data: 评论响应数据
        :return: 标签字符串，用逗号分隔
        """
        try:
            tags = []
            if not comment_data or "data" not in comment_data:
                return ""
            
            filter_list = comment_data["data"].get("filterList", [])
            if not filter_list:
                return ""
            
            # 只获取第一个 filter_item
            first_filter = filter_list[0]
            sub_tags = first_filter.get("subTag", [])
                
                # 遍历子标签，排除"最新"标签
            for tag in sub_tags:
                tag_name = tag.get("filterName", "")
                tag_count = tag.get("filterCount", 0)
                if tag_name and tag_name != "最新" and tag_count > 0:
                    tags.append(f"{tag_name}({tag_count})")
            
            return "，".join(tags) if tags else ""
            
        except Exception as e:
            self.logger.error("提取评论标签失败", e)
            return ""

    def _load_progress(self) -> Dict:
        """加载爬取进度"""
        default_progress = {
            'current_city_index': 0,  # 当前城市索引
            'processed_cities': [],  # 已处理的城市代码列表
            'processed_hotel_ids': [],  # 全局已处理的酒店ID列表
            'last_city_total': 0,  # 上一个城市的总酒店数
            'last_city_processed': 0,  # 上一个城市已处理的酒店数
        }

        if not self.progress_file.exists():
            try:
                self.progress_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self.progress_file, "w", encoding="utf-8") as f:
                    json.dump(default_progress, f, ensure_ascii=False, indent=4)
                self.logger.info(
                    "创建进度文件成功",
                    extra={'emoji': EMOJI['SUCCESS']}
                )
            except Exception as e:
                self.logger.error(
                    f"创建进度文件失败: {str(e)}",
                    extra={'emoji': EMOJI['ERROR']}
                )
            return default_progress

        try:
            with open(self.progress_file, "r", encoding="utf-8") as f:
                progress = json.load(f)
                
            if not isinstance(progress, dict):
                raise ValueError("进度数据格式错误")
            
            # 确保所有必需字段都存在
            for field in default_progress:
                if field not in progress:
                    progress[field] = default_progress[field]
            
            self.logger.info(
                f"加载进度文件成功: 当前城市={CITIES[progress['current_city_index']]['zhname']}, "
                f"已处理酒店数={len(progress['processed_hotel_ids'])}",
                extra={'emoji': EMOJI['SUCCESS']}
            )
            return progress
            
        except Exception as e:
            self.logger.error(f"加载进度文件失败: {str(e)}")
            return default_progress

    def _save_progress(self, city_index: int = None, hotel_id: str = None, force: bool = False):
        """
        保存爬取进度
        :param city_index: 当前城市索引
        :param hotel_id: 酒店ID
        :param force: 是否强制保存
        """
        try:
            if hotel_id:
                self.processed_hotels.add(hotel_id)
                self.save_counter += 1

            if city_index is not None:
                self.current_city_index = city_index

            # 只有在以下情况下才保存进度：
            # 1. 强制保存
            # 2. 处理了指定数量的酒店
            # 3. 完成了一个城市的处理
            if not (force or self.save_counter >= self.SAVE_INTERVAL or city_index != self.current_city_index):
                return

            progress = {
                'current_city_index': self.current_city_index,
                'processed_cities': list(self.processed_cities),
                'processed_hotel_ids': list(self.processed_hotels),
                'last_city_total': self.progress.get('last_city_total', 0),
                'last_city_processed': self.progress.get('last_city_processed', 0)
            }
            
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(progress, f, ensure_ascii=False, indent=4)
            
            self.save_counter = 0  # 重置计数器
            
            self.logger.info(
                f"保存进度成功: 城市={CITIES[self.current_city_index]['zhname'] if self.current_city_index < len(CITIES) else '完成'}, "
                f"已处理酒店数={len(self.processed_hotels)}",
                extra={'emoji': EMOJI['SUCCESS']}
            )
        except Exception as e:
            self.logger.error(f"保存进度失败: {str(e)}")

    def _is_city_processed(self, city_code: str) -> bool:
        """检查城市是否已处理"""
        try:
            city_index = next(i for i, city in enumerate(CITIES) if city['code'] == city_code)
        except StopIteration:
            return False
            
        return city_index < self.current_city_index and city_code in self.processed_cities

    def _is_hotel_processed(self, hotel_id: str) -> bool:
        """检查酒店是否已处理"""
        return hotel_id in self.processed_hotels

    def _handle_interrupt(self):
        """处理中断事件"""
        self.is_interrupted = True
        # 创建中断标志文件
        INTERRUPT_FILE.touch()
        # 强制保存当前进度
        self._save_progress(force=True)
        self.logger.warning(
            "检测到中断信号,已保存进度,正在退出...",
            extra={'emoji': "⚠️"}
        )
        sys.exit(0)

    def log_hotel_info(self, hotel_info: Dict):
        """记录酒店信息"""
        self.logger.info(
            f"\n============酒店============\n"
            f"{EMOJI['HOTEL']} 酒店名称：{hotel_info.get('酒店名称', '')}\n"
            f"{EMOJI['HOTEL']} 酒店英文名称：{hotel_info.get('酒店英文名称', '')}\n"
            f"{EMOJI['SCORE']} 总体评分：{hotel_info.get('总评分', '')}\n"
            f"{EMOJI['SCORE']} 卫生评分：{hotel_info.get('卫生评分', '')}\n"
            f"{EMOJI['SCORE']} 环境评分：{hotel_info.get('设施评分', '')}\n"
            f"{EMOJI['SCORE']} 服务评分：{hotel_info.get('服务评分', '')}\n"
            f"{EMOJI['SCORE']} 设施评分：{hotel_info.get('设施评分', '')}\n"
            f"{EMOJI['SCORE']} 酒店标签：{hotel_info.get('酒店标签', '')}\n"
            f"{EMOJI['COMMENT']} AI评论：{hotel_info.get('AI评论', '')}\n"
            f"\n==============================",
            extra={'emoji': EMOJI['HOTEL']}
        )

    def log_comment_info(self, hotel_name: str, hotel_en_name: str, comment: Dict):
        """记录评论信息"""
        self.logger.info(
            f"\n=============评论=============\n"
            f"{EMOJI['HOTEL']} 酒店名称：{hotel_name}\n"
            f"{EMOJI['HOTEL']} 酒店英文名称：{hotel_en_name}\n"
            f"{EMOJI['SCORE']} 评论ID：{comment.get('评论ID', '')}\n"
            f"{EMOJI['SCORE']} 用户名：{comment.get('用户名', '')}\n"
            f"{EMOJI['SCORE']} 用户等级：{comment.get('用户等级', '')}\n"
            f"{EMOJI['SCORE']} 评分：{comment.get('评分', '')}\n"
            f"{EMOJI['IMAGE']} 评论内容：{comment.get('评论内容', '')}\n"
            f"{EMOJI['SCORE']} 评论时间：{comment.get('评论时间', '')}\n"
            f"{EMOJI['SCORE']} 房型：{comment.get('房型', '')}\n"
            f"{EMOJI['SCORE']} 出行类型：{comment.get('出行类型', '')}\n"
            f"{EMOJI['SCORE']} 酒店回复：{comment.get('酒店回复', '')}\n"
            f"{EMOJI['SCORE']} 评论来源：{comment.get('评论来源', '')}\n"
            f"{EMOJI['SCORE']} 有用数：{comment.get('有用数', '')}\n"
            f"{EMOJI['IMAGE']} 评论图片：{comment.get('评论图片', '')}\n"
            f"\n============================",
            extra={'emoji': EMOJI['COMMENT']}
        )

    def log_progress(self, current: int, total: int, type_name: str):
        """记录进度信息"""
        self.logger.info(
            f"进度: {type_name}({current}/{total})",
            extra={'emoji': EMOJI['PROGRESS']}
        )

    def log_error(self, message: str, error: Exception = None):
        """记录错误信息"""
        self.logger.error(
            f"{message}, {str(error) if error else ''}",
            exc_info=error if error else None
        )

    def _check_interrupt(self) -> bool:
        """检查是否需要中断"""
        return self.is_interrupted or INTERRUPT_FILE.exists()

    def save_hotel_data(self, hotel_info: Dict):
        """保存酒店数据到数据库"""
        try:
            hotel_data = {
                'hotel_id': str(hotel_info['酒店ID']),
                'name': hotel_info['酒店名称'],
                'name_en': hotel_info['酒店英文名称'],
                'address': hotel_info['地址'],
                'location': hotel_info['地理位置'],
                'star': hotel_info['酒店等级'],
                'tags': hotel_info['酒店标签'],
                'main_tag': hotel_info['评论主标签'],
                'traffic_info': hotel_info['交通信息'],
                'rating_all': float(hotel_info.get('总评分', 0)),
                'rating_location': float(hotel_info.get('位置评分', 0)),
                'rating_facility': float(hotel_info.get('设施评分', 0)),
                'rating_service': float(hotel_info.get('服务评分', 0)),
                'rating_hygiene': float(hotel_info.get('卫生评分', 0)),
                'rating_cost': float(hotel_info.get('性价比评分', 0)),
                'rating_desc': hotel_info.get('评分描述', ''),
                'comment_count': int(hotel_info.get('总评论数', 0)),
                'city_name': hotel_info['城市名称'],
                'updated_at': datetime.now()
            }
            
            # 检查酒店是否已存在
            try:
                hotel = ElongHotel.get_by_id(hotel_data['hotel_id'])
                # 如果存在，更新数据
                query = ElongHotel.update(**hotel_data).where(ElongHotel.hotel_id == hotel_data['hotel_id'])
                query.execute()
            except ElongHotel.DoesNotExist:
                # 如果不存在，创建新记录
                ElongHotel.create(**hotel_data)
            
            self.logger.info(
                f"保存酒店数据成功: {hotel_info['酒店名称']}",
                extra={'emoji': EMOJI['SUCCESS']}
            )
            
        except Exception as e:
            self.logger.error(f"保存酒店数据失败: {str(e)}")
            raise  # 向上抛出异常，让事务回滚

    def save_comment_data(self, comments: List[Dict]):
        """保存评论数据到数据库"""
        try:
            for comment in comments:
                try:
                    comment_time = datetime.strptime(comment['评论时间'], "%Y-%m-%d %H:%M:%S") if comment['评论时间'] else None
                    checkin_time = datetime.strptime(comment['入住时间'], "%Y-%m-%d %H:%M:%S") if comment['入住时间'] else None
                    reply_time = datetime.strptime(comment['回复时间'], "%Y-%m-%d %H:%M:%S") if comment['回复时间'] else None
                    
                    comment_data = {
                        'comment_id': str(comment['评论ID']),
                        'hotel_id': str(comment['酒店ID']),
                        'user_name': comment['用户名'],
                        'user_level': int(comment['用户等级']) if comment['用户等级'] else None,
                        'rating': float(comment['评分']) if comment['评分'] else 0.0,
                        'content': comment['评论内容'],
                        'comment_time': comment_time,
                        'useful_count': int(comment['有用数']) if comment['有用数'] else 0,
                        'travel_type': comment['出行类型'],
                        'room_type': comment['房型'],
                        'checkin_time': checkin_time,
                        'source': comment['评论来源'],
                        'ip_location': comment['IP归属地'],
                        'images': comment['评论图片'],
                        'local_images': comment['本地图片'],
                        'hotel_reply': comment['酒店回复'],
                        'reply_time': reply_time
                    }
                    
                    # 检查评论是否已存在
                    try:
                        comment_obj = ElongComment.get_by_id(comment_data['comment_id'])
                        # 如果存在，更新数据
                        query = ElongComment.update(**comment_data).where(ElongComment.comment_id == comment_data['comment_id'])
                        query.execute()
                    except ElongComment.DoesNotExist:
                        # 如果不存在，创建新记录
                        ElongComment.create(**comment_data)
                    
                except Exception as e:
                    self.logger.error(f"保存单条评论数据失败: {str(e)}")
                    continue

            self.logger.info(
                f"保存评论数据成功: {len(comments)} 条",
                extra={'emoji': EMOJI['SUCCESS']}
            )

        except Exception as e:
            self.logger.error(f"保存评论数据失败: {str(e)}")
            raise  # 向上抛出异常，让事务回滚

    def get_city_hotels(self, city_info: Dict, check_in: str, check_out: str) -> List[Dict]:
        """获取城市的所有酒店列表"""
        all_hotels = []
        page = 0

        try:
            # 获取第一页，确定总数
            first_result = self.get_hotel_list(city_info["code"], check_in, check_out, page)
            if not first_result or "data" not in first_result:
                return all_hotels

            total_hotels = first_result["data"].get("hotelCount", 0)
            if total_hotels == 0:
                return all_hotels

            self.logger.info(
                f"{EMOJI['CONTINUE']} 城市 {city_info['zhname']} 共有 {total_hotels} 家酒店"
            )

            # 处理第一页数据
            if "hotelList" in first_result["data"]:
                all_hotels.extend(first_result["data"]["hotelList"])

            # 获取剩余页面
            while True:
                if self._check_interrupt():
                    break

                if (page + 1) * PAGE_SIZE >= total_hotels:
                    break

                page += 1
                self.logger.info(f"正在获取第 {page + 1} 页酒店列表")
                
                result = self.get_hotel_list(city_info["code"], check_in, check_out, page)
                if not result or "data" not in result or not result["data"].get("hotelList"):
                    break

                all_hotels.extend(result["data"]["hotelList"])
                time.sleep(REQUEST_DELAY)

            return all_hotels

        except Exception as e:
            self.logger.error(f"获取城市 {city_info['zhname']} 的酒店列表失败: {str(e)}")
            return all_hotels

    def process_hotel_comments(self, hotel_id: str, hotel_name: str, hotel_en_name: str):
        """处理酒店的评论数据"""
        try:
            # 1. 先获取评分信息，获取真实的评论总数
            score_result = self.get_hotel_score_info(hotel_id)
            if not score_result:
                self.logger.error(f"获取酒店 {hotel_name} 评分信息失败")
                return

            total_comments = score_result.get("总评论数", 0)
            if total_comments == 0:
                self.logger.info(f"酒店 {hotel_name} 暂无评论")
                return

            total_pages = (total_comments + COMMENT_PAGE_SIZE - 1) // COMMENT_PAGE_SIZE
            self.logger.info(f"酒店 {hotel_name} 共有 {total_comments} 条评论，{total_pages} 页")

            # 2. 处理评论页面
            page = 0
            while True:
                if self._check_interrupt():
                    break

                try:
                    # 获取当前页评论
                    self.logger.info(f"正在获取第 {page + 1}/{total_pages} 页评论")
                    result = self.get_hotel_comments(hotel_id, page, COMMENT_PAGE_SIZE)
                    if not result or "data" not in result:
                        page += 1
                        if page >= total_pages:
                            break
                        continue

                    # 3. 处理当前页评论
                    comments = result["data"].get("comments", [])
                    if not comments:
                        page += 1
                        if page >= total_pages:
                            break
                        continue

                    # 4. 解析评论数据
                    page_comments = []
                    for comment_data in comments:
                        comment_info = self.parse_comment_info(
                            comment_data,
                            hotel_id=hotel_id,
                            hotel_name=hotel_name,
                            hotel_en_name=hotel_en_name
                        )
                        if comment_info:
                            page_comments.append(comment_info)

                    # 5. 保存当前页评论
                    if page_comments:
                        self.save_data(comments=page_comments)
                        self.logger.info(
                            f"保存第 {page + 1} 页评论成功: {len(page_comments)} 条",
                            extra={'emoji': EMOJI['SUCCESS']}
                        )

                    # 检查是否还有下一页
                    page += 1
                    if page >= total_pages:
                        break

                    time.sleep(COMMENT_REQUEST_DELAY)

                except Exception as e:
                    self.logger.error(f"处理第 {page + 1} 页评论失败: {str(e)}")
                    page += 1
                    if page >= total_pages:
                        break
                    continue

            self.logger.info(
                f"完成酒店 {hotel_name} 的评论采集",
                extra={'emoji': EMOJI['COMPLETE']}
            )

        except Exception as e:
            self.logger.error(f"处理酒店 {hotel_name} 的评论失败: {str(e)}")

    def process_city(self, city_info: Dict, city_index: int) -> None:
        """处理单个城市的数据"""
        try:
            if self._is_city_processed(city_info['code']):
                self.logger.info(
                    f"{EMOJI['SKIP']} 城市 {city_info['zhname']} 已处理，跳过"
                )
                return

            self.logger.info(
                f"\n{EMOJI['CONTINUE']} 开始爬取城市: {city_info['zhname']} ({city_info['enname']})"
            )

            # 设置日期范围
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            next_day = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d")

            # 1. 获取第一页酒店列表，确定总数
            page = 0
            first_result = self.get_hotel_list(city_info["code"], tomorrow, next_day, page)
            if not first_result or "data" not in first_result:
                self.logger.warning(f"城市 {city_info['zhname']} 获取酒店列表失败")
                return

            total_hotels = first_result["data"].get("hotelCount", 0)
            if total_hotels == 0:
                self.logger.warning(f"城市 {city_info['zhname']} 没有找到酒店")
                return

            self.logger.info(
                f"{EMOJI['CONTINUE']} 城市 {city_info['zhname']} 共有 {total_hotels} 家酒店"
            )

            self.progress['last_city_total'] = total_hotels
            self.progress['last_city_processed'] = 0
            processed_count = 0

            # 2. 遍历所有页面
            while True:
                if self._check_interrupt():
                    break

                # 获取当前页酒店列表
                if page > 0:
                    self.logger.info(f"正在获取第 {page + 1} 页酒店列表")
                    result = self.get_hotel_list(city_info["code"], tomorrow, next_day, page)
                    if not result or "data" not in result:
                        break
                else:
                    result = first_result

                # 处理当前页的酒店
                hotels = result["data"].get("hotelList", [])
                if not hotels:
                    break

                # 3. 处理当前页的每个酒店
                for hotel_data in hotels:
                    if self._check_interrupt():
                        break

                    try:
                        # 3.1 获取酒店基本信息
                        hotel_info = self.parse_hotel_info(hotel_data)
                        hotel_id = str(hotel_info["酒店ID"])

                        if self._is_hotel_processed(hotel_id):
                            self.logger.info(
                                f"{EMOJI['SKIP']} 酒店 {hotel_info['酒店名称']} 已处理，跳过"
                            )
                            continue

                        # 3.2 获取评分和评论信息
                        score_result = self.get_hotel_score_info(hotel_id)
                        if score_result:
                            # 更新酒店评分信息
                            hotel_info.update({
                                "总评分": score_result.get("总评分", 0),
                                "位置评分": score_result.get("位置评分", 0),
                                "设施评分": score_result.get("设施评分", 0),
                                "服务评分": score_result.get("服务评分", 0),
                                "卫生评分": score_result.get("卫生评分", 0),
                                "性价比评分": score_result.get("性价比评分", 0),
                                "评分描述": score_result.get("评分描述", ""),
                                "评论数量": score_result.get("总评论数", 0),
                                "好评率": score_result.get("好评率", 0),
                                "好评数": score_result.get("好评数", 0),
                                "差评数": score_result.get("差评数", 0),
                                "AI评论": score_result.get("AI评论", ""),
                            })
                        
                        self.log_hotel_info(hotel_info)

                        # 3.3 保存酒店信息
                        self.save_data(hotels=[hotel_info])

                        # 3.4 处理评论数据
                        total_comments = score_result.get("总评论数", 0)
                        if total_comments > 0:
                            total_pages = (total_comments + COMMENT_PAGE_SIZE - 1) // COMMENT_PAGE_SIZE
                            self.logger.info(f"酒店 {hotel_info['酒店名称']} 共有 {total_comments} 条评论，{total_pages} 页")

                            # 获取所有评论
                            comment_page = 0
                            while comment_page < total_pages:
                                if self._check_interrupt():
                                    break

                                try:
                                    # 获取当前页评论
                                    self.logger.info(f"正在获取第 {comment_page + 1}/{total_pages} 页评论")
                                    comment_result = self.get_hotel_comments(hotel_id, comment_page, COMMENT_PAGE_SIZE)
                                    if not comment_result or "data" not in comment_result:
                                        comment_page += 1
                                        continue

                                    # 处理当前页评论
                                    comments = comment_result["data"].get("comments", [])
                                    if not comments:
                                        comment_page += 1
                                        continue

                                    # 解析评论数据
                                    page_comments = []
                                    for comment_data in comments:
                                        comment_info = self.parse_comment_info(
                                            comment_data,
                                            hotel_id=hotel_id,
                                            hotel_name=hotel_info["酒店名称"],
                                            hotel_en_name=hotel_info["酒店英文名称"]
                                        )
                                        if comment_info:
                                            page_comments.append(comment_info)

                                    # 保存当前页评论
                                    if page_comments:
                                        self.save_data(comments=page_comments)
                                        self.logger.info(
                                            f"保存第 {comment_page + 1} 页评论成功: {len(page_comments)} 条",
                                            extra={'emoji': EMOJI['SUCCESS']}
                                        )

                                    comment_page += 1
                                    time.sleep(COMMENT_REQUEST_DELAY)

                                except Exception as e:
                                    self.logger.error(f"处理第 {comment_page + 1} 页评论失败: {str(e)}")
                                    comment_page += 1
                                    continue

                            self.logger.info(
                                f"完成酒店 {hotel_info['酒店名称']} 的评论采集",
                                extra={'emoji': EMOJI['COMPLETE']}
                            )
                        else:
                            self.logger.info(f"酒店 {hotel_info['酒店名称']} 暂无评论")

                        # 3.5 更新进度
                        processed_count += 1
                        self.progress['last_city_processed'] = processed_count
                        self.log_progress(processed_count, total_hotels, "酒店采集")
                        self._save_progress(city_index, hotel_id)

                    except Exception as e:
                        self.logger.error(f"处理酒店时出错: {str(e)}")
                        continue

                    time.sleep(REQUEST_DELAY)

                # 检查是否还有下一页
                if (page + 1) * PAGE_SIZE >= total_hotels:
                    break

                page += 1
                time.sleep(REQUEST_DELAY)

            # 4. 完成城市处理
            if processed_count >= total_hotels:
                self.processed_cities.add(city_info['code'])
            self._save_progress(city_index + 1, force=True)

            self.logger.info(
                f"完成城市 {city_info['zhname']} 的采集，共处理 {processed_count}/{total_hotels} 家酒店",
                extra={'emoji': EMOJI['COMPLETE']}
            )

        except Exception as e:
            self.logger.error(f"处理城市 {city_info['zhname']} 时出错: {str(e)}")

# 修改主函数
if __name__ == "__main__":
    crawler = ElongCrawler()
    
    try:
        # 加载进度
        progress = crawler._load_progress()
        current_city_index = progress['current_city_index']

        # 遍历城市列表
        for index, city_info in enumerate(CITIES):
            if crawler._check_interrupt():
                break

            # 处理单个城市
            crawler.process_city(city_info, index)
            time.sleep(2)
            
    except KeyboardInterrupt:
        crawler._handle_interrupt()
    finally:
        if INTERRUPT_FILE.exists():
            INTERRUPT_FILE.unlink()
    
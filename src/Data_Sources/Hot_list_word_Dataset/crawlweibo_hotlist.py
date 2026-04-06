#这个文件是用来爬取微博热搜榜单的

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import configparser
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
import re
import json
import os

async def search_weibo_dayhotlist(date):
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url=f"https://github.com/lonnyzhang423/weibo-hot-hub/blob/main/archives/{date}.md",
        )
        if result.success:
            pattern = r"## 热门搜索\n(.*?)(?=## |$)"
            match = re.search(pattern, result.markdown, re.DOTALL)
            if match:
                weibo_hot_list = match.group(1).strip().split("\n")
                
                weibo_hot_list = [
                    {
                        "title": re.search(r"\[(.*?)\]", item).group(1) if re.search(r"\[(.*?)\]", item) else item.strip().split(". ", 1)[-1],
                        "url": re.search(r"\<(.*?)\>", item).group(1) if re.search(r"\<(.*?)\>", item) else ""
                    }
                    for item in weibo_hot_list
                ]
                    
                weibo_hotsearch_list = weibo_hot_list[1:]
                return weibo_hotsearch_list
            return []
        else:
            print(type(result.markdown))
            print(f"{date}爬取Error:", result.error_message)
async def fetch_weibo_hotlist(start_date, end_date):
    start_year = start_date.year
    start_month = start_date.month
    start_day = start_date.day
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    start_date = datetime(start_year, start_month, start_day)
    end_date = datetime(end_year, end_month, end_day)
    current_date = start_date
    hotlist_dict = {}

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        print(type(date_str))
        print(f"Fetching hotlist for {date_str}")
        hotlist = await search_weibo_dayhotlist(date_str)
        if hotlist:
            hotlist_dict[date_str]=[]
            for title in hotlist:
                hotlist_dict[date_str].append({"title": title, "is_AIGC": False})
        current_date += timedelta(days=1)
    
    return hotlist_dict

BASE_DIR = Path(__file__).resolve().parents[3]
CONFIG_PATH = os.path.join(BASE_DIR, "config", "Data_Sources-Hot_list_word_Dataset-config.ini")
parser = configparser.ConfigParser()
parser.read(CONFIG_PATH, encoding="utf-8")

# 调用函数获取配置中的微博热搜榜单时间范围
start_date = datetime.strptime(parser.get("Weibo", "start_date", fallback="2021-01-07"), "%Y-%m-%d")
end_date = datetime.strptime(parser.get("Weibo", "end_date", fallback="2024-01-05"), "%Y-%m-%d")
hotlist_dict = asyncio.run(fetch_weibo_hotlist(start_date, end_date))
# 构建目标文件路径
hotlist_path = os.path.join(
    BASE_DIR, parser.get("Weibo", "output_file", fallback="download_dir/Hot_list_word/Weibo/weibo_hotlist.json")
)
os.makedirs(os.path.dirname(hotlist_path), exist_ok=True)
# 将结果保存为JSON文件
with open(hotlist_path, "w", encoding="utf-8") as json_file:
    json.dump(hotlist_dict, json_file, ensure_ascii=False, indent=4)

print(f"微博热搜榜单已保存为 {hotlist_path}")

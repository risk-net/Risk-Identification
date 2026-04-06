#这个文件是用来爬取知乎热搜榜单的

import asyncio
import configparser
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re

from crawl4ai import AsyncWebCrawler

async def search_zhihu_dayhotlist(date):
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url=f"https://github.com/lonnyzhang423/zhihu-hot-hub/blob/main/archives/{date}.md",
        )
        if result.success:
            pattern1 = r"## (热门搜索|热搜)\n(.*?)(?=## |$)"
            match1 = re.search(pattern1, result.markdown, re.DOTALL)
            print(match1)
            if match1:
                zhihu_hotsearch_list = match1.group(1).strip().split("\n")
                
                zhihu_hot_list = [
                    {
                        "title": re.search(r"\[(.*?)\]", item).group(1) if re.search(r"\[(.*?)\]", item) else item.strip().split(". ", 1)[-1],
                        "url": re.search(r"\<(.*?)\>", item).group(1) if re.search(r"\<(.*?)\>", item) else ""
                    }
                    for item in zhihu_hotsearch_list
                ]
                    
                zhihu_hotsearch = zhihu_hotsearch_list[1:]
            pattern2 = r"## 热门话题\n(.*?)(?=## |$)"
            match2 = re.search(pattern2, result.markdown, re.DOTALL)
            if match2:
                zhihu_hottalk_list = match2.group(1).strip().split("\n")
                
                zhihu_hottalk_list = [
                    {
                        "title": re.search(r"\[(.*?)\]", item).group(1) if re.search(r"\[(.*?)\]", item) else item.strip().split(". ", 1)[-1],
                        "url": re.search(r"\<(.*?)\>", item).group(1) if re.search(r"\<(.*?)\>", item) else ""
                    }
                    for item in zhihu_hottalk_list
                ]
                    
                zhihu_hottalk = zhihu_hottalk_list[1:]
                zhihu_hot_list = zhihu_hotsearch + zhihu_hottalk
                return zhihu_hot_list
            return []
        else:
            print(type(result.markdown))
            print(f"{date}爬取Error:", result.error_message)
async def fetch_zhihu_hotlist(start_date, end_date):
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
        hotlist = await search_zhihu_dayhotlist(date_str)
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

start_date = datetime.strptime(parser.get("Weibo", "start_date", fallback="2021-01-07"), "%Y-%m-%d")
end_date = datetime.strptime(parser.get("Weibo", "end_date", fallback="2024-01-05"), "%Y-%m-%d")
hotlist_dict = asyncio.run(fetch_zhihu_hotlist(start_date, end_date))

hotlist_path = os.path.join(
    BASE_DIR,
    parser.get("Zhihu", "output_file", fallback="download_dir/Hot_list_word/Zhihu/zhihu_hotlist.json"),
)
os.makedirs(os.path.dirname(hotlist_path), exist_ok=True)
with open(hotlist_path, "w", encoding="utf-8") as json_file:
    json.dump(hotlist_dict, json_file, ensure_ascii=False, indent=4)

print(f"知乎热搜榜单已保存为 {hotlist_path}")

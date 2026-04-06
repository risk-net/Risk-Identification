#该文件是为了获取OpenNewsArchive的新闻数据 文件为wrac格式，下载至指定文件夹
import configparser
from pathlib import Path

import openxlab
from openxlab.dataset import info
from openxlab.dataset import get
from openxlab.dataset import download
import os

BASE_DIR = Path(__file__).resolve().parents[3]
CONFIG_PATH = os.path.join(BASE_DIR, "config", "Data_Sources-OpenNewsArchive-config.ini")
parser = configparser.ConfigParser()
parser.read(CONFIG_PATH, encoding="utf-8")

dataset_repo = parser.get("OpenNewsArchive", "dataset_repo", fallback="OpenDataLab/OpenNewsArchive")
access_key = parser.get("OpenNewsArchive", "access_key", fallback="<Access Key>")
secret_key = parser.get("OpenNewsArchive", "secret_key", fallback="<Secret Key>")
source_path = parser.get("OpenNewsArchive", "source_path", fallback="/README.md")

openxlab.login(ak=access_key, sk=secret_key)
info(dataset_repo=dataset_repo)
# 构建目标文件路径
target_path = os.path.join(
    BASE_DIR, parser.get("OpenNewsArchive", "target_path", fallback="download_dir/OpenNewsArchive")
)
os.makedirs(target_path, exist_ok=True)
get(dataset_repo=dataset_repo, target_path=target_path)
download(dataset_repo=dataset_repo, source_path=source_path, target_path=target_path)

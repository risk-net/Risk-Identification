# Crawled Dataset

`Crawled_Dataset/` 保存面向自定义新闻站点的网页爬虫脚本。它主要用于补充人工指定站点的新闻数据，不属于论文方法复现的硬性前置步骤。

## Current Structure

- `news_web_crawler.py`
  - 主入口
- `auto_crawler.py`
  - 自动调度入口
- `crawler_utils.py`
  - 配置读取、结果保存、上传等通用函数
- `crawlers/`
  - 各站点的具体爬虫实现

当前仓库中已经包含若干站点实现，例如：

- `36kr`
- `人民网`
- `腾讯新闻`
- `澎湃新闻`
- `新华网`

## Configuration

对应配置文件：

- `config/Data_Sources-Crawled_Dataset-config.ini`

主要配置项包括：

- `[NewsSites]`
  - 站点基础 URL
- `[Keywords]`
  - 各站点关键词
- `[CrawlerMapping]`
  - 站点名到具体爬虫函数的映射
- `[MaxPages]`
  - 各站点翻页数或滚动次数
- `[Manual]`
  - 手动模式下是否自动上传
- `[TestMode]`
  - 测试模式开关与测试站点
- `[AutoCrawler]`
  - 自动调度开关和时间
- `[API]`
  - 可选的上传接口地址

## Usage

从仓库根目录执行：

```bash
python src/Data_Sources/Crawled_Dataset/news_web_crawler.py
```

测试模式：

```bash
python src/Data_Sources/Crawled_Dataset/news_web_crawler.py --test
```

自动模式：

```bash
python src/Data_Sources/Crawled_Dataset/news_web_crawler.py --auto
```

## Public Notes

- 这类脚本高度依赖网页结构
- 网站改版后，`crawlers/` 下的实现可能需要同步修改
- 公开版保留这些脚本，是为了说明数据获取方式，不保证所有站点长期免修改可运行

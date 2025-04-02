import os
import time
import pdfkit
import urllib.parse
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

class WebsiteCrawler:
    def __init__(self, start_url, max_depth=3, output_dir='pdf_output'):
        self.start_url = start_url
        self.max_depth = max_depth
        self.output_dir = output_dir
        self.visited_urls = set()
        self.domain = urlparse(start_url).netloc
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # PDFKit 配置
        self.wkhtmltopdf_path = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'  # 修改为你的实际路径
        self.config = pdfkit.configuration(wkhtmltopdf=self.wkhtmltopdf_path)
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
    
    def is_valid_url(self, url):
        """检查URL是否有效且属于同一域名"""
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.netloc == self.domain and parsed.scheme in ['http', 'https']
    
    def get_links(self, url):
        """获取页面中的所有有效链接"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = set()
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(url, href)
                if self.is_valid_url(full_url):
                    links.add(full_url)
            return links
        except Exception as e:
            print(f"Error getting links from {url}: {e}")
            return set()
    
    def save_as_pdf(self, url):
        """将网页保存为PDF，处理包含中文的URL"""
        try:
            # 解析URL
            parsed_url = urlparse(url)
            
            # 对路径和查询参数进行编码处理（保留结构）
            path = parsed_url.path.strip('/') or 'index'
            path_part = urllib.parse.quote(path, safe='')  # 编码路径部分
            path_part = path_part.replace('%2F', '_')  # 将编码后的斜杠替换为下划线
            
            # 处理查询参数部分
            query_part = parsed_url.query
            if query_part:
                # 编码查询参数，但保留参数结构（=和&）
                query_part = urllib.parse.quote_plus(query_part, safe='=&')
                query_part = query_part.replace('%3D', '_eq_').replace('%26', '_and_')
                # 限制查询参数长度
                query_part = query_part[:50]
            
            # 构建文件名
            if query_part:
                filename = f"{parsed_url.netloc}_{path_part}_{query_part}.pdf"
            else:
                filename = f"{parsed_url.netloc}_{path_part}.pdf"
            
            # 进一步清理文件名（替换Windows不允许的字符）
            invalid_chars = r'<>:"/\|?*'
            for char in invalid_chars:
                filename = filename.replace(char, '_')
            
            # 限制文件名长度
            filename = filename[:200] + '.pdf' if len(filename) > 200 else filename
            
            filepath = os.path.join(self.output_dir, filename)
            
            # 使用pdfkit转换为PDF
            pdfkit.from_url(url, filepath, configuration=self.config)
            print(f"Saved: {url} as {filepath}")
            return True
        except Exception as e:
            print(f"Error saving {url} as PDF: {e}")
            return False
    
    def crawl(self, url, depth=1):
        """递归爬取网页"""
        if depth > self.max_depth or url in self.visited_urls:
            return
        
        self.visited_urls.add(url)
        print(f"Crawling: {url} (Depth: {depth})")
        
        # 保存当前页面为PDF
        self.save_as_pdf(url)
        
        # 获取并处理子链接
        if depth < self.max_depth:
            links = self.get_links(url)
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for link in links:
                    if link not in self.visited_urls:
                        futures.append(executor.submit(self.crawl, link, depth + 1))
                
                for future in as_completed(futures):
                    future.result()  # 等待所有任务完成
    
    def start(self):
        """开始爬取"""
        self.crawl(self.start_url)
        print(f"\nCrawling completed. Total pages saved: {len(self.visited_urls)}")

if __name__ == "__main__":
    # 使用示例
    # start_url = "https://skyroveroptics.com/"  # 替换为你要爬取的网站
    start_url = "http://www.sky-rover.com/" 
    crawler = WebsiteCrawler(start_url, max_depth=3)
    crawler.start()
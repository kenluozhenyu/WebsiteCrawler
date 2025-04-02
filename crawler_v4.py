import os
import time
import pdfkit
from urllib.parse import urljoin, urlparse, unquote, quote
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

class WebsiteCrawler:
    def __init__(self, start_url, max_depth=3, output_dir='pdf_output'):
        # 初始化时解码start_url中的中文（如果有）
        self.start_url = self._normalize_url(start_url)
        self.max_depth = max_depth
        self.output_dir = output_dir
        self.visited_urls = set()
        self.domain = urlparse(self.start_url).netloc
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # PDFKit配置
        self.wkhtmltopdf_path = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
        self.config = pdfkit.configuration(wkhtmltopdf=self.wkhtmltopdf_path)
        
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _normalize_url(self, url):
        """统一URL格式，处理中文编码问题"""
        parsed = urlparse(url)
        # 解码路径和查询参数中的中文
        path = unquote(parsed.path)
        query = unquote(parsed.query)
        # 重新编码（确保URL格式正确）
        path = quote(path)
        query = quote(query, safe='=&')
        # 重构URL
        return parsed._replace(path=path, query=query).geturl()
    
    def is_valid_url(self, url):
        """检查URL是否有效且属于同一域名"""
        try:
            parsed = urlparse(self._normalize_url(url))
            return bool(parsed.netloc) and parsed.netloc == self.domain and parsed.scheme in ['http', 'https']
        except:
            return False
    
    def get_links(self, url):
        """获取页面中的所有有效链接，正确处理中文"""
        try:
            normalized_url = self._normalize_url(url)
            response = self.session.get(normalized_url, timeout=10)
            response.raise_for_status()
            
            # 设置正确的编码（根据网页实际编码调整）
            if response.encoding.lower() not in ('utf-8', 'gbk', 'gb2312'):
                response.encoding = 'utf-8'
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links = set()
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                try:
                    # 处理相对路径和绝对路径
                    full_url = urljoin(normalized_url, href)
                    # 标准化URL（处理中文）
                    full_url = self._normalize_url(full_url)
                    if self.is_valid_url(full_url):
                        links.add(full_url)
                except Exception as e:
                    print(f"Error processing link {href}: {e}")
            return links
        except Exception as e:
            print(f"Error getting links from {url}: {e}")
            return set()
    
    def save_as_pdf(self, url):
        """将网页保存为PDF，正确处理中文URL"""
        try:
            normalized_url = self._normalize_url(url)
            parsed_url = urlparse(normalized_url)
            
            # 创建文件名（保留中文可读性）
            path = unquote(parsed_url.path).strip('/') or 'index'
            filename = f"{parsed_url.netloc}_{path}"
            
            # 添加查询参数（如果有）
            if parsed_url.query:
                query = unquote(parsed_url.query)
                filename += f"_{query}"
            
            # 替换特殊字符
            filename = (
                filename.replace('/', '_')
                .replace('?', '_')
                .replace('=', '_eq_')
                .replace('&', '_and_')
            )
            
            # 进一步清理文件名
            invalid_chars = r'<>:"\|*'
            for char in invalid_chars:
                filename = filename.replace(char, '_')
            
            # 限制长度并添加扩展名
            filename = filename[:200] + '.pdf' if len(filename) > 200 else filename + '.pdf'
            filepath = os.path.join(self.output_dir, filename)
            
            # 保存PDF
            pdfkit.from_url(normalized_url, filepath, configuration=self.config)
            print(f"Saved: {normalized_url} as {filepath}")
            return True
        except Exception as e:
            print(f"Error saving {url} as PDF: {e}")
            return False
    
    def crawl(self, url, depth=1):
        """递归爬取网页，正确处理中文URL"""
        normalized_url = self._normalize_url(url)
        
        if depth > self.max_depth or normalized_url in self.visited_urls:
            return
        
        self.visited_urls.add(normalized_url)
        print(f"Crawling: {normalized_url} (Depth: {depth})")
        
        # 保存当前页面为PDF
        self.save_as_pdf(normalized_url)
        
        # 获取并处理子链接
        if depth < self.max_depth:
            links = self.get_links(normalized_url)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for link in links:
                    if link not in self.visited_urls:
                        futures.append(executor.submit(self.crawl, link, depth + 1))
                
                for future in as_completed(futures):
                    future.result()
    
    def start(self):
        """开始爬取"""
        self.crawl(self.start_url)
        print(f"\nCrawling completed. Total pages saved: {len(self.visited_urls)}")

if __name__ == "__main__":
    # 测试中文URL
    # start_url = "http://www.sky-rover.com/"
    start_url = "https://skyroveroptics.com/"
    crawler = WebsiteCrawler(start_url, max_depth=2)
    crawler.start()

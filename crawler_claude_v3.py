import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urlparse, urljoin, quote, unquote, parse_qs
import pdfkit
import logging
import re
import hashlib
import concurrent.futures
from threading import Lock
import queue

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("crawler.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class WebCrawler:
    def __init__(self, start_url, max_depth=3, delay=1, output_folder="crawled_pdfs", max_workers=10):
        self.start_url = start_url
        self.max_depth = max_depth
        self.delay = delay
        self.output_folder = output_folder
        self.max_workers = max_workers
        
        # 使用线程安全的集合和锁
        self.visited_urls = set()
        self.visited_lock = Lock()
        self.url_queue = queue.Queue()
        
        # 用于跟踪已生成的文件名，避免重复
        self.used_filenames = set()
        self.filename_lock = Lock()
        
        self.base_domain = urlparse(start_url).netloc

        # PDFKit 配置
        self.wkhtmltopdf_path = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'  # 修改为你的实际路径
        self.config = pdfkit.configuration(wkhtmltopdf=self.wkhtmltopdf_path)
        
        # 创建输出目录
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
            
        # 计数器
        self.processed_count = 0
        self.processed_lock = Lock()
    
    def is_valid_url(self, url):
        """检查URL是否有效且属于同一域名"""
        parsed = urlparse(url)
        return bool(parsed.netloc) and parsed.netloc == self.base_domain
    
    def get_page_links(self, url):
        """获取页面中的所有链接"""
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            response.encoding = response.apparent_encoding  # 自动检测编码
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links = []
                
                for a_tag in soup.find_all('a', href=True):
                    link = a_tag['href']
                    
                    # 处理可能含有中文字符的URL
                    try:
                        link = unquote(link)  # 先解码，防止重复编码
                        # 只对非ASCII字符进行编码
                        link = ''.join([quote(c) if ord(c) > 127 else c for c in link])
                    except Exception as e:
                        logger.warning(f"URL编码处理异常: {link}, 错误: {str(e)}")
                    
                    link = urljoin(url, link)
                    
                    # 排除锚点链接
                    if '#' in link:
                        link = link.split('#')[0]
                    
                    # 使用锁检查URL是否已访问
                    with self.visited_lock:
                        if link and self.is_valid_url(link) and link not in self.visited_urls:
                            links.append(link)
                
                return links
            else:
                logger.error(f"无法获取页面 {url}，状态码: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"获取 {url} 链接时发生错误: {str(e)}")
            return []
    
    def generate_safe_filename(self, url, depth):
        """生成安全且唯一的文件名，保留尽可能多的URL信息"""
        try:
            # 解码URL中可能的编码字符
            decoded_url = unquote(url)
            
            # 解析URL各部分
            parsed_url = urlparse(decoded_url)
            domain = parsed_url.netloc
            path = parsed_url.path.strip('/')
            query = parsed_url.query
            
            # 从查询参数中提取关键信息
            query_info = ""
            if query:
                query_params = parse_qs(query)
                # 尝试提取常见的ID参数
                common_id_params = ['id', 'page', 'article', 'post', 'item', 'p']
                for param in common_id_params:
                    if param in query_params:
                        query_info += f"_{param}-{query_params[param][0]}"
            
            # 如果路径为空，使用"index"
            if not path:
                path = "index"
            else:
                # 保留路径中的关键部分，移除文件扩展名
                path_parts = path.split('/')
                if len(path_parts) > 3:
                    # 如果路径很长，只保留最后几个部分
                    path = '_'.join(path_parts[-3:])
                path = re.sub(r'\.(html|php|jsp|asp|aspx)$', '', path)
            
            # 创建基本文件名
            base_name = f"{domain}_{path}{query_info}_d{depth}"
            
            # 移除文件名中的非法字符
            safe_name = re.sub(r'[<>:"/\\|?*\s]', '_', base_name)
            
            # 确保文件名不超过合理长度，但保留更多信息
            if len(safe_name) > 180:
                # 保留域名和路径的前部分，添加哈希值确保唯一性
                url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
                safe_name = f"{safe_name[:160]}_{url_hash}_d{depth}"
            else:
                # 添加URL部分哈希以确保唯一性
                url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
                safe_name = f"{safe_name}_{url_hash}"
            
            # 确保文件名唯一
            with self.filename_lock:
                original_name = safe_name
                counter = 0
                while safe_name in self.used_filenames:
                    counter += 1
                    safe_name = f"{original_name}_{counter}"
                
                self.used_filenames.add(safe_name)
            
            return safe_name
            
        except Exception as e:
            # 如果生成文件名出错，使用完整URL的哈希值作为文件名
            logger.warning(f"生成文件名异常，使用完整哈希: {str(e)}")
            url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
            with self.filename_lock:
                safe_name = f"page_{url_hash}_d{depth}"
                # 确保即使哈希也不重复
                original_name = safe_name
                counter = 0
                while safe_name in self.used_filenames:
                    counter += 1
                    safe_name = f"{original_name}_{counter}"
                self.used_filenames.add(safe_name)
            return safe_name
    
    def save_as_pdf(self, url, filename):
        """将页面保存为PDF"""
        try:
            options = {
                'page-size': 'A4',
                'encoding': "UTF-8",
                'custom-header': [
                    ('User-Agent', 'Mozilla/5.0')
                ],
                'no-outline': None,
                'quiet': ''
            }
            
            pdf_path = os.path.join(self.output_folder, f"{filename}.pdf")
            
            pdfkit.from_url(url, pdf_path, options=options, configuration=self.config)
            logger.info(f"保存PDF成功: {pdf_path}")
            return True
        except Exception as e:
            logger.error(f"保存PDF失败 {url}: {str(e)}")
            return False
    
    def process_url(self, url_info):
        """处理单个URL的爬取任务"""
        url, depth = url_info
        
        # 如果超过最大深度，直接返回
        if depth > self.max_depth:
            return
        
        # 检查URL是否已访问
        with self.visited_lock:
            if url in self.visited_urls:
                return
            self.visited_urls.add(url)
        
        logger.info(f"正在爬取 ({depth}/{self.max_depth}): {url}")
        
        # 生成安全的文件名
        filename = self.generate_safe_filename(url, depth)
        
        # 保存为PDF
        self.save_as_pdf(url, filename)
        
        # 更新计数器
        with self.processed_lock:
            self.processed_count += 1
        
        # 如果未达到最大深度，获取并添加链接到队列
        if depth < self.max_depth:
            links = self.get_page_links(url)
            for link in links:
                self.url_queue.put((link, depth + 1))
    
    def crawl(self):
        """使用线程池执行爬虫任务"""
        # 将起始URL加入队列
        self.url_queue.put((self.start_url, 0))
        
        # 创建线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            while True:
                # 获取当前队列中所有URL并提交到线程池
                current_urls = []
                try:
                    # 先尝试获取一个URL（这会阻塞直到有URL）
                    current_urls.append(self.url_queue.get(timeout=1))
                    
                    # 尝试获取队列中剩余的所有URL（非阻塞）
                    while True:
                        try:
                            current_urls.append(self.url_queue.get(block=False))
                        except queue.Empty:
                            break
                except queue.Empty:
                    # 如果队列为空且所有任务都已完成，则退出循环
                    if all(future.done() for future in futures):
                        break
                    continue
                
                # 提交任务到线程池
                for url_info in current_urls:
                    # 添加随机延迟，避免请求过于密集
                    time.sleep(self.delay * (0.5 + 0.5 * (hash(url_info[0]) % 100) / 100))
                    future = executor.submit(self.process_url, url_info)
                    futures.append(future)
                
                # 清理已完成的future
                futures = [f for f in futures if not f.done()]
                
                # 进度更新
                with self.visited_lock:
                    logger.info(f"进度: 已处理 {self.processed_count} 个页面, 已访问 {len(self.visited_urls)} 个URL, 队列中还有 {self.url_queue.qsize()} 个页面待处理")

def get_optimal_thread_count():
    """根据CPU核心数确定合适的线程数"""
    import multiprocessing
    cpu_count = multiprocessing.cpu_count()
    # 网络IO密集型任务，线程数可以设置为CPU核心数的2-4倍
    return min(cpu_count * 3, 20)  # 设置上限为20个线程

if __name__ == "__main__":
    try:
        # 设置起始URL和参数
        # start_url = input("请输入起始URL (例如 https://example.com): ")
        start_url = "http://www.sky-rover.com/"
        
        try:
            max_depth = int(input("请输入最大爬取深度 (默认3): ") or "3")
        except ValueError:
            max_depth = 3
            print("输入的深度无效，使用默认值3")
        
        try:
            delay = float(input("请输入请求延迟秒数 (默认1): ") or "1")
        except ValueError:
            delay = 1
            print("输入的延迟无效，使用默认值1秒")
        
        try:
            max_workers = int(input(f"请输入线程数 (默认{get_optimal_thread_count()}): ") or str(get_optimal_thread_count()))
        except ValueError:
            max_workers = get_optimal_thread_count()
            print(f"输入的线程数无效，使用默认值{max_workers}")
        
        # output_folder = input("请输入保存PDF的文件夹路径 (默认'crawled_pdfs'): ") or "crawled_pdfs"
        output_folder = "crawled_pdfs"
        
        print("\n开始爬取网站...")
        start_time = time.time()
        
        # 创建并启动爬虫
        crawler = WebCrawler(
            start_url=start_url,
            max_depth=max_depth,
            delay=delay,
            output_folder=output_folder,
            max_workers=max_workers
        )
        crawler.crawl()
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"爬取完成。共爬取 {len(crawler.visited_urls)} 个页面，用时 {duration:.2f} 秒。")
        logger.info(f"PDF文件保存在目录: {os.path.abspath(crawler.output_folder)}")
        
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
    except Exception as e:
        logger.critical(f"程序执行中发生严重错误: {str(e)}")
        logger.exception("详细错误信息:")
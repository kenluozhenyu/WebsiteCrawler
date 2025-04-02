import os
import time
import random
import base64
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from bs4 import BeautifulSoup

class ConfluenceCrawler:
    def __init__(self, base_url, username, password, space_key, max_depth=3, output_dir="confluence_pdfs"):
        """
        初始化爬虫
        :param base_url: Confluence 基础URL (如 "https://wiki.your-company.com")
        :param username: 用户名
        :param password: 密码或API Token
        :param space_key: 空间Key (如 "DEV")
        :param max_depth: 最大爬取深度
        :param output_dir: PDF输出目录
        """
        self.base_url = base_url.rstrip('/')
        self.space_key = space_key
        self.max_depth = max_depth
        self.output_dir = output_dir
        self.visited_urls = set()
        self.session = requests.Session()
        
        # 认证配置
        self.session.auth = (username, password)  # Basic Auth
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml'
        })
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Selenium 配置
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-blink-features=AutomationControlled')

    def _normalize_url(self, url):
        """标准化URL（处理相对路径和锚点）"""
        if url.startswith('/'):
            return urljoin(self.base_url, url)
        return url

    def _get_pdf_filename(self, url):
        """生成PDF文件名"""
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.split('/') if p]
        filename = f"{parsed.netloc}_{'_'.join(path_parts[-2:])}.pdf"
        
        # 清理无效字符
        invalid_chars = r'<>:"/\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        return filename[:200]  # 限制长度

    def save_as_pdf(self, url):
        """使用Selenium保存页面为PDF"""
        filename = self._get_pdf_filename(url)
        filepath = os.path.join(self.output_dir, filename)
        
        driver = webdriver.Chrome(options=self.chrome_options)
        try:
            print(f"正在转换: {url}")
            driver.get(url)
            
            # 等待主要内容加载完成
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.main-content'))
            )
            # 隐藏无关元素
            driver.execute_script("""
                document.querySelectorAll('.sidebar, .header, .footer').forEach(el => el.style.display = 'none');
                document.querySelector('.main-content').style.margin = '0';
            """)
            
            # 打印参数
            print_params = {
                'landscape': False,
                'displayHeaderFooter': False,
                'printBackground': True,
                'preferCSSPageSize': True,
                'margin': {'top': '0.4in', 'bottom': '0.4in', 'left': '0.4in', 'right': '0.4in'}
            }
            
            # 生成PDF
            pdf_data = driver.execute_cdp_cmd('Page.printToPDF', print_params)
            with open(filepath, 'wb') as f:
                f.write(base64.b64decode(pdf_data['data']))
            
            print(f"已保存: {filepath}")
            return True
        except Exception as e:
            print(f"PDF转换失败: {url} - {str(e)}")
            return False
        finally:
            driver.quit()

    def get_page_links(self, url):
        """获取页面中的所有有效链接"""
        try:
            # 随机延迟防止被封
            time.sleep(random.uniform(1, 3))
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 只提取内容区域的链接
            links = set()
            main_content = soup.select_one('#main-content, .content-container')
            if not main_content:
                return set()
                
            for a in main_content.find_all('a', href=True):
                href = a['href']
                if href.startswith(('/display/', '/pages/')):
                    full_url = self._normalize_url(href)
                    if full_url not in self.visited_urls:
                        links.add(full_url)
            return links
        except Exception as e:
            print(f"链接提取失败: {url} - {str(e)}")
            return set()

    def crawl(self, url, current_depth=1):
        """递归爬取页面"""
        if current_depth > self.max_depth or url in self.visited_urls:
            return
        
        self.visited_urls.add(url)
        print(f"\n深度 {current_depth}: 爬取 {url}")
        
        # 保存当前页为PDF
        self.save_as_pdf(url)
        
        # 获取并处理子链接
        if current_depth < self.max_depth:
            child_links = self.get_page_links(url)
            for link in child_links:
                self.crawl(link, current_depth + 1)

    def start(self):
        """启动爬虫"""
        start_url = f"{self.base_url}/display/{self.space_key}"
        print(f"开始爬取空间: {self.space_key} (起始URL: {start_url})")
        self.crawl(start_url)
        print(f"\n爬取完成! 共保存 {len(self.visited_urls)} 个PDF文件到 {self.output_dir}")

if __name__ == "__main__":
    # 配置参数
    CONFLUENCE_URL = "https://wiki.your-company.com"
    USERNAME = "your-username"
    PASSWORD = "your-password-or-api-token"  # 或用API Token
    SPACE_KEY = "DEV"  # Confluence空间Key
    
    # 启动爬虫
    crawler = ConfluenceCrawler(
        base_url=CONFLUENCE_URL,
        username=USERNAME,
        password=PASSWORD,
        space_key=SPACE_KEY,
        max_depth=3
    )
    crawler.start()

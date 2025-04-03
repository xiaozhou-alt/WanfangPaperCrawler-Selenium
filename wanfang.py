import os
import re
import random
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from tqdm import tqdm
import urllib
import json
from random import uniform
from time import sleep


def safe_navigate(driver, url):
    driver.get(url)
    sleep(uniform(1.5, 3.5))  # 设置1.5-3.5秒随机等待


class WanfangPaperCrawler:
    '''万方数据论文爬虫（标题、作者、摘要、关键词）'''

    def __init__(self):
        self.base_url = 'https://s.wanfangdata.com.cn'
        self.driver = self.init_browser()
        self.output_dir = 'output'
        os.makedirs(self.output_dir, exist_ok=True)

    def init_browser(self):
        """初始化浏览器"""
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # 调试时建议先关闭无头模式
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                """
            })
            return driver
        except Exception as e:
            print(f"浏览器初始化失败: {str(e)}")
            exit(1)

    def random_delay(self, min_sec=2.5, max_sec=5):
        """随机延迟"""
        time.sleep(random.uniform(min_sec, max_sec))
    

    def search_papers(self, keyword, max_papers=10, file_format='csv'):
        """搜索论文"""
        data = []
        try:
            # 构建带多年度+期刊类型筛选的URL（核心修改）
            base_params = {
                "q": keyword,
                "facet": [
                    {  # 时间筛选条件
                        "PublishYear": {
                            "title": "年份",
                            "label": ["2022", "2023", "2024", "2025"],
                            "value": ["2022", "2023", "2024", "2025"]
                        }
                    },
                    {  # 新增期刊类型筛选
                        "Type": {
                            "title": "资源类型",
                            "label": ["期刊论文"],
                            "value": ["Periodical"]  # 万方期刊论文的API标识
                        }
                    }
                ]
            }
            encoded_facet = urllib.parse.quote(json.dumps(base_params['facet']))
            search_url = f"{self.base_url}/paper?q={urllib.parse.quote(keyword)}&facet={encoded_facet}"
            
            print(f"🌐 正在访问搜索页面: {search_url}")
            self.driver.get(search_url)
            self.random_delay(2.5, 5)

            # 验证筛选条件（新增检测点）
            try:
                # 检测年份筛选
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                    '[data-filter-key="PublishYear"] .ivu-checkbox-wrapper-checked'))
                )
                
                # 检测期刊类型筛选（新增）
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR,
                    '[data-filter-key="Type"] .ivu-checkbox-wrapper-checked'))
                )
                print("✅ 多年度+期刊类型过滤条件已生效")
            except TimeoutException:
                print("⚠️ 未检测到完整过滤条件，可能筛选未生效")

            papers_per_page = 20
            total_pages = (max_papers + papers_per_page - 1) // papers_per_page

            # 生成分页URL列表（修正循环逻辑）
            page_urls = [f"{search_url}&p={page}" for page in range(1, total_pages + 1)]

            collected = 0
            current_page = 1
            
            while True:
                print(f"\n正在处理第 {current_page} 页")
                # self.driver.save_screenshot(f'page_{current_page}_debug.png')  # 调试用
                
                # 获取当前页论文列表（增加显式等待）
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.title-area'))
                )
                papers = self.driver.find_elements(By.CSS_SELECTOR, '.title-area')
                print(f"本页找到 {len(papers)} 篇论文")


                for paper in tqdm(papers, desc="处理论文"):
                    if collected >= max_papers:
                        break

                    try:
                        # 获取论文标题元素
                        title_element = paper.find_element(By.CSS_SELECTOR, '.title')

                        # 获取论文链接 - 通过点击标题进入详情页
                        title = title_element.text.strip()
                        print(f"\n处理论文: {title[:50]}...")

                        title_element.click()
                        self.random_delay(2, 3)

                        # 切换到新打开的标签页
                        if len(self.driver.window_handles) > 1:
                            self.driver.switch_to.window(self.driver.window_handles[1])
                            current_url = self.driver.current_url

                            # 获取论文详情
                            paper_info = self.get_paper_details()
                            if paper_info:
                                data.append(paper_info)
                                collected += 1
                                print(f"[成功] 已收集 {collected}/{max_papers} 篇论文")
                                self.random_delay()

                            # 关闭详情页标签并切换回列表页
                            self.driver.close()
                            self.driver.switch_to.window(self.driver.window_handles[0])
                        else:
                            print("未能成功打开论文详情页")
                            continue

                    except Exception as e:
                        print(f"处理论文时出错: {str(e)}")
                        # 增强窗口状态检查（网页7建议）
                        current_handles = self.driver.window_handles
                        if len(current_handles) > 1:
                            try:
                                self.driver.switch_to.window(current_handles[1])
                                self.driver.close()
                            except Exception:
                                pass
                            finally:
                                self.driver.switch_to.window(current_handles[0])
                if collected >= max_papers:
                    break

                # 模拟点击下一页按钮 (关键修改)
                try:
                    # Step 1: 定位下一页按钮（根据你提供的DOM结构）
                    next_btn = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.next'))
                    )

                    # Step 2: 滚动到分页区域（确保按钮可见）
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    self.random_delay(1, 1.5)  # 短延迟让滚动完成

                    # Step 3: 直接点击（无需ActionChains）
                    next_btn.click()

                    # Step 4: 验证是否翻页成功（通过检测active页码）
                    WebDriverWait(self.driver, 15).until(
                        EC.text_to_be_present_in_element(
                            (By.CSS_SELECTOR, 'span.pager.active'),
                            str(current_page + 1)
                        )
                    )
                    current_page += 1
                    print(f"✅ 成功跳转到第 {current_page} 页")

                except TimeoutException:
                    print("⚠️ 已到达最后一页")
                    break
                except Exception as e:
                    print(f"翻页失败: {str(e)}")
                    break

            # 保存数据
            if data:
                self.save_data(data, keyword, file_format)

        except Exception as e:
            print(f"搜索过程中出错: {str(e)}")
        finally:
            if data:
                self.save_data(data, keyword, file_format)

    def get_paper_details(self):
        """获取论文详情（包含完整年份信息）"""
        try:
            # 等待包含年份的元素加载
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.itemUrl'))
            )

            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')

            # 标题（保持原有逻辑）
            title_element = soup.select_one('.detailTitleCN span')
            title = title_element.text.strip() if title_element else ''

            # 年份信息提取
            year = "未知"
        
            # Case 1: 处理学位论文格式
            thesis_year_div = soup.find('div', class_='thesisYear list')
            if thesis_year_div:
                item_url = thesis_year_div.find('div', class_='itemUrl')
                if item_url:
                    year_span = item_url.find('span')
                    if year_span and year_span.text.strip().isdigit():
                        year = year_span.text.strip()

            # Case 2: 处理期刊论文格式
            if year == "未知":
                publish_div = soup.find('div', class_='publish list')
                if publish_div:
                    item_url_div = publish_div.find('div', class_='itemUrl')
                    if item_url_div:
                        date_text = item_url_div.text.strip()
                        date_match = re.search(r'(?:(\d{4})-\d{2}-\d{2})|(\d{4})', date_text)
                        year = date_match.group(1) or date_match.group(2) if date_match else "未知"

            # ========== 新增分类号提取逻辑 ==========
            classification = "未知"
            classify_div = soup.find('div', class_='classify list')
            
            if classify_div:
                # 情况1：学术论文结构（带multi-sep层）
                multi_sep_span = classify_div.find('span', class_='multi-sep')
                if multi_sep_span:
                    classification = multi_sep_span.find('span').text.strip()
                
                # 情况2：期刊结构（直接包含分类号）
                else:
                    item_url_div = classify_div.find('div', class_='itemUrl')
                    if item_url_div:
                        classification = item_url_div.find('span').text.strip()

                # 清理分类号格式（移除括号内容）
                classification = re.sub(r'$.*?$', '', classification).strip()
            
            journal_info = {}
            journal_div = soup.find('div', class_='publish list')

            if journal_div:
                # 期刊名称提取（支持两种DOM结构）
                journal_name = (
                    journal_div.find('a', class_='journalName') or 
                    journal_div.find('span', class_=re.compile('journal-name'))
                )
                if journal_name:
                    journal_info['期刊名称'] = re.sub(r'[\u25a0\u2588]', '', journal_name.text.strip())  # 去除特殊方块字符
                
                # ISSN提取（增强正则匹配）
                issn_span = journal_div.find(lambda tag: tag.name == 'span' and 'ISSN' in tag.text)
                if issn_span:
                    issn_text = re.sub(r'[^0-9X-]', '', issn_span.text.split('：')[-1])
                    journal_info['ISSN'] = issn_text[:9] if len(issn_text) > 9 else issn_text  # 标准化长度

            # # 合并到论文信息
            # paper_info.update({
            #     **journal_info,
            #     '文献类型': '期刊论文'  # 明确标注文献类型
            # })

            # ========== 作者单位提取逻辑 ==========
            organizations = []

            # 核心选择器路径（适配动态类名）
            org_elements = soup.select('div.organization.detailOrganization a[class*="test-detail-org"]')

            # 备用选择器（处理可能的类名变体）
            if not org_elements:
                org_elements = soup.select('div[class*="detailOrganization"] span.multi-sep a')

            # 文本清洗逻辑
            for org in org_elements:
                # 分割复合单位（示例："北京邮电大学；网络空间安全学院"）
                units = [u.strip() for u in org.text.split('；')]
                # 移除地址信息（示例："北京100876"）
                clean_units = [re.sub(r'，\s*\S+$', '', u) for u in units]
                organizations.extend(clean_units)

            # 去重处理
            organizations = list(set(organizations))

            # 作者、摘要、关键词保持原有逻辑
            authors = []
            author_elements = soup.select('.author.detailTitle .test-detail-author span')
            for author in author_elements:
                author_text = author.get_text(strip=True)
                if author_text:
                    authors.append(author_text)

            abstract_element = soup.select_one('.summary.list .text-overflow span')
            abstract = abstract_element.text.strip() if abstract_element else ''

            keywords = []
            keyword_elements = soup.select('.keyword.list .multi-sep span')
            for word in keyword_elements:
                keywords.append(word.text.strip())

            paper_info = {
                '标题': title,
                '论文发表年份': year,
                **{k: v for k, v in journal_info.items() if v},  # 动态合并有效期刊字段
                '分类号': classification,
                '作者': ', '.join(authors) or '未知',
                '单位': '; '.join(organizations) if organizations else '未知',
                '摘要': abstract,
                '关键词': '; '.join(keywords)
            }

            print(f"成功获取论文信息 | 出版年份: {year}")
            return paper_info

        except Exception as e:
            print(f"获取论文详情时出错: {str(e)}")
            return None

    def save_data(self, data, keyword, file_format='csv'):
        """保存数据到文件"""
        if not data:
            print("没有数据需要保存")
            return

        # 创建DataFrame
        df = pd.DataFrame(data)

        # 生成文件名
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"wanfang_papers_{keyword}_{timestamp}"

        if file_format.lower() == 'tsv':
            filepath = os.path.join(self.output_dir, f"{filename}.tsv")
            df.to_csv(filepath, sep='\t', index=False, encoding='utf-8')
            print(f"数据已保存为TSV文件: {filepath}")
        else:
            filepath = os.path.join(self.output_dir, f"{filename}.csv")
            df.to_csv(filepath, index=False, encoding='utf_8_sig')
            print(f"数据已保存为CSV文件: {filepath}")

    def run(self):
        """运行爬虫"""
        try:
            print("\n=== 万方数据论文爬虫 ===")
            keyword = input("请输入搜索关键词: ").strip()
            if not keyword:
                print("必须输入搜索关键词!")
                return

            max_papers = input("请输入要爬取的最大论文数(默认10): ").strip()
            max_papers = int(max_papers) if max_papers.isdigit() else 10

            file_format = input("请选择保存格式 (1)CSV (2)TSV (默认CSV): ").strip()
            file_format = 'tsv' if file_format == '2' else 'csv'

            print(f"\n开始爬取论文，关键词: {keyword}")
            self.search_papers(keyword, max_papers, file_format)

        except Exception as e:
            print(f"\n运行过程中出错: {str(e)}")
        finally:
            self.driver.quit()
            print("\n程序结束")


if __name__ == '__main__':
    crawler = WanfangPaperCrawler()
    crawler.run()
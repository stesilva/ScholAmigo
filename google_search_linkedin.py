from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import urllib.parse
import random
import time
import re

def parse_arguments():
    parser = argparse.ArgumentParser(description='Scrape LinkedIn Profiles from Google Search')
    parser.add_argument('--keyword', type=str, required=True, help='Search keywords for LinkedIn profiles')
    parser.add_argument('--pages', type=int, default=5, help='Number of Google pages to scrape')
    parser.add_argument('--output', type=str, default='data/linkedin_profiles.csv', help='Output CSV filename')
    return parser.parse_args()

class LinkedInScraper:
    def __init__(self, keyword: str, pages: int):
        self.keyword = urllib.parse.quote_plus(keyword)
        self.pages = pages
        self.results = []
        self.driver = self._init_driver()
        self.seen_urls = set()

    def _init_driver(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]
        
        options = webdriver.ChromeOptions()
        options.add_argument(f"user-agent={random.choice(user_agents)}")
        options.add_argument("--disable-blink-features=AutomationControlled")

        
        service = Service(executable_path="D:/Downloads/chromedriver-win64/chromedriver-win64/chromedriver.exe")
        return webdriver.Chrome(service=service, options=options)

    def _random_delay(self):
        time.sleep(random.uniform(1.5, 4.5))

    def search(self):
        base_url = f"https://www.google.com/search?num=100q=site%3Alinkedin.com/in%20{self.keyword}"
        
        for page in range(self.pages):
            try:
                self.driver.get(f"{base_url}&start={page*10}")
                self._random_delay()
                self._handle_cookie_warning()
                self._parse_page()
            except Exception as e:
                print(f"Error on page {page+1}: {str(e)}")
                break

    def _handle_cookie_warning(self):
        try:
            WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept')]"))
            ).click()
            self._random_delay()
        except:
            pass

    def _parse_page(self):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Improved selector for LinkedIn profiles
        for result in soup.select('div.g'):
            link_element = result.select_one('a[href*="linkedin.com/in/"]')
            if not link_element:
                continue
                
            profile_url = link_element['href']
            if profile_url in self.seen_urls:
                continue
                
            title = self._clean_title(link_element.text)
            if not title:
                title = result.select_one('h3').text if result.select_one('h3') else "No Title"
                
            self.results.append({
                "title": title,
                "url": profile_url
            })
            self.seen_urls.add(profile_url)

    def _clean_title(self, text: str) -> str:
        patterns = [
            r" - .*? LinkedIn",
            r" \| LinkedIn",
            r"LinkedIn.*$"
        ]
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        return text.strip()

    def save_results(self,args):
        df = pd.DataFrame(self.results)
        df.drop_duplicates(subset=['url'], inplace=True)
        df.to_csv(args.output, index=False, encoding='utf-8')

def main():
    args = parse_arguments()
    scraper = LinkedInScraper(keyword=args.keyword, pages=args.pages)
    scraper.search()
    scraper.save_results(args)
    
    print(f"Scraped {len(scraper.results)} unique LinkedIn profiles")
    print(f"Results saved to {args.output}")

if __name__ == "__main__":
    main()

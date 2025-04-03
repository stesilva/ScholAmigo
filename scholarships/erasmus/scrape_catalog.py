import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.eacea.ec.europa.eu/scholarships/erasmus-mundus-catalogue_en?page="

def get_scholarships(page_count: int = 11) -> pd.DataFrame:
    all_scholarships = []
    
    for page_num in range(page_count):
        url = BASE_URL + str(page_num)
        print(f"[ScrapeCatalog] Fetching {url} ...")
        
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"[ScrapeCatalog] Error on page {page_num}: {e}")
            continue
        
        soup = BeautifulSoup(resp.text, "html.parser")
        article_divs = soup.find_all(
            "div",
            class_="ecl-content-item-block__item contextual-region ecl-u-mb-l ecl-col-12"
        )
        
        for div in article_divs:
            title_span = div.find("span", class_="ecl-link__label")
            link_a    = div.find("a", class_="ecl-link ecl-link--standalone ecl-link--icon")
            desc_div  = div.find("div", class_="ecl-content-block__description")
            
            title = title_span.get_text(strip=True) if title_span else None
            link  = link_a.get("href") if link_a else None
            
            if link and title:
                all_scholarships.append({"title": title, "link": link})
    
    df = pd.DataFrame(all_scholarships)
    print(f"[ScrapeCatalog] Total scholarships found: {len(df)}")
    return df

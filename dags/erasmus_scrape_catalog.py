import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://www.eacea.ec.europa.eu/scholarships/erasmus-mundus-catalogue_en?page="

#scrapes scholarship titles and links from the Erasmus Mundus scholarship catalog
def get_scholarships(page_count: int = 11):
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
            
            title = title_span.get_text(strip=True) if title_span else None
            link  = link_a.get("href") if link_a else None
            
            if link and title:
                all_scholarships.append({"title": title, "link": link})
    
    df = pd.DataFrame(all_scholarships)
    print(f"[ScrapeCatalog] Total scholarships found: {len(df)}")
    return df

#filters the scraped df to include only specific target scholarships based on predefined URLs
def target_scholarships(df: pd.DataFrame):
    target_scholarships = [
        'https://resound-ma.eu/',
        'https://master-misei.com',
        'https://master-digicrea.univ-st-etienne.fr/',
        'https://www.response-able-futures.eu/',
        'https://www.internationalmastercriminology.eu/',
        'https://densys.univ-lorraine.fr/',
        'https://www.filmmemory.eu/',
        'https://www.imt-atlantique.fr/en/study/masters/emjmd/sarena',
        'https://www.unite-codas-master.eu/',
        'https://www.emjm-imaging.eu/',
        'https://www.replaymasters.eu/',
        'http://www.EMship.eu',
        'https://em-sufonama.eu',
        'https://imsoglo.eu/',
        'https://www.master-meta4-0.eu',
        'https://morphophen.eu/',
        'https://master-emmah.eu',
        'https://mesd.edu.umontpellier.fr/',
        'http://www.erasmusmundus-archmat.eu/',
        'http://www.emle.org/',
        'https://pioneer-master.eu/',
        'https://imatec-mundus.eu/',
        'https://www.sdsi.ma/',
        'https://geoplanet-impg.eu',
        'http://www.master-goals.eu/'
    ]
    
    return df[df['link'].isin(target_scholarships)].reset_index(drop=True)
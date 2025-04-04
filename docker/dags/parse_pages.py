import requests
from bs4 import BeautifulSoup
import pandas as pd

RELEVANT_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6", "p"]

def parse_page_structured(project: str, main_link: str, subpage_url: str):
    try:
        response = requests.get(subpage_url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        #print(f"[ParsePages] Error fetching {subpage_url}: {e}")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    data_blocks = []
    
    for elem in soup.find_all(RELEVANT_TAGS):
        text_content = elem.get_text(strip=True)
        if text_content:
            data_blocks.append({
                "project": project,
                "main_link": main_link,
                "subpage": subpage_url,
                "tag": elem.name,
                "text": text_content
            })
    return data_blocks

def parse_all_subpages(df: pd.DataFrame):
    all_structured = []
    for _, row in df.iterrows():
        project     = row["program"]
        main_link   = row["main_link"]
        subpage_url = row["subpages"]
        
        blocks = parse_page_structured(project, main_link, subpage_url)
        all_structured.extend(blocks)
    
    return all_structured

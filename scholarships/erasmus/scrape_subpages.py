import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd

def get_subpages(df: pd.DataFrame, end_row: int = 100):
    results = {}
    end_row = min(end_row, len(df))
    
    for i in range(end_row):
        main_url = df.loc[i, "link"]
        title    = df.loc[i, "title"]
        
        print(f"[ScrapeSubpages] Processing row {i}, main_url={main_url}")
        
        try:
            resp = requests.get(main_url, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            #print(f"[ScrapeSubpages]  Request error for {main_url}: {e}")
            continue
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        menu = None
        for nav_tag in soup.find_all("nav"):
            id_attr    = nav_tag.get("id", "")
            class_attr = " ".join(nav_tag.get("class", []))
            if "menu" in id_attr.lower() or "menu" in class_attr.lower():
                menu = nav_tag
                break
        
        if not menu:
            for ul_tag in soup.find_all("ul"):
                id_attr    = ul_tag.get("id", "")
                class_attr = " ".join(ul_tag.get("class", []))
                if "menu" in id_attr.lower() or "menu" in class_attr.lower():
                    menu = ul_tag
                    break
        
        menu_links = []
        if menu:
            menu_links = [urljoin(main_url, a["href"]) 
                          for a in menu.find_all("a", href=True)]
        else:
            #print("[ScrapeSubpages]  Could not find a 'menu'—fallback to top-level link approach.")
            all_links = soup.find_all("a", href=True)
            for a in all_links:
                candidate = urljoin(main_url, a["href"])
                if urlparse(candidate).netloc == urlparse(main_url).netloc:
                    if candidate != main_url and "#" not in candidate:
                        menu_links.append(candidate)
        
        menu_links = list(set(menu_links))
        
        visited = set()
        for link_url in menu_links:
            if link_url in visited:
                continue
            visited.add(link_url)
            
            try:
                r = requests.get(link_url, timeout=10)
                if r.status_code != 200:
                    #print(f"[ScrapeSubpages]    Failed subpage {link_url} (status {r.status_code})")
                    continue
            except Exception as e:
                #print(f"[ScrapeSubpages]    Request error for subpage {link_url}: {e}")
                continue
        
        results[i] = [title, main_url, len(visited), visited]
        print(f"[ScrapeSubpages] Done with row {i}: processed {len(visited)} subpages.")
    
    records = []
    for key, value in results.items():
        title        = value[0]
        main_link    = value[1]
        num_subpages = value[2]
        subpages     = list(value[3])
        
        records.append({
            "id": key,
            "program": title,
            "main_link": main_link,
            "num_subpages": num_subpages,
            "subpages": subpages
        })
    subpages_df = pd.DataFrame(records)
    subpages_df[["subpages", "num_subpages"]] = subpages_df.apply(_prepend_main_link, axis=1)
    
    return subpages_df


def _prepend_main_link(row: pd.Series):
    main_link = row["main_link"].rstrip("/")
    subs      = [sp.rstrip("/") for sp in row["subpages"]]
    
    if main_link not in subs:
        new_subpages = [row["main_link"]] + row["subpages"]
    else:
        new_subpages = row["subpages"]
    
    return pd.Series({
        "subpages": new_subpages,
        "num_subpages": len(new_subpages)
    })

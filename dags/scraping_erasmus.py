from datetime import datetime
from erasmus_scrape_catalog import get_scholarships, target_scholarships
from erasmus_scrape_subpages import get_subpages
from erasmus_clean_links import clean_subpages #filter_by_subpage_count
from erasmus_parse_pages import parse_all_subpages
from send_data_to_aws import send_data_to_aws

def run_pipeline():
    #scrape the scholarship catalog
    df_catalog = target_scholarships(get_scholarships(page_count=11))
    #gather subpages from each scholarship site
    subpages_df = get_subpages(df_catalog, end_row=100)
    #clean out anchor links, update subpage counts
    cleaned_df = clean_subpages(subpages_df)
    #parse each subpage
    structured_data = parse_all_subpages(cleaned_df)
    #save to json
    timestamp = datetime.now().strftime("%Y-%m")
    filename  = f"{timestamp}_erasmus_scholarship_data.json"
    send_data_to_aws(structured_data, "scholarship-data-bdm", filename)
    
if __name__ == "__main__":
    run_pipeline()

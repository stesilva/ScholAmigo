import pandas as pd
import re

def process_linkedin_data(csv_file):
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: File not found at {csv_file}")
        return pd.DataFrame()

    def normalize_linkedin_url(url):
        if isinstance(url, str):
            match = re.search(r"/in/([a-zA-Z0-9-]+)/?$", url)
            if match:
                username = match.group(1)
                return f"https://www.linkedin.com/in/{username}"
        return None

    df['normalized_url'] = df['url'].apply(normalize_linkedin_url)
    df = df[df['normalized_url'].notna()]

    if not df.empty:
        df = df[['title', 'normalized_url']].rename(columns={'normalized_url': 'url'})
    else:
        print("No valid LinkedIn URLs found in the file.")

    return df

csv_file_path = "data/linkedin_profiles.csv"
result_df = process_linkedin_data(csv_file_path)

if not result_df.empty:
    print(result_df)
    result_df.to_csv("data/normalized_linkedin_data.csv", index=False)
else:
    print("No data to save.")

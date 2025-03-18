import pandas as pd
import re

def process_linkedin_data(csv_file):
    """
    Reads a CSV file, normalizes LinkedIn URLs to a standard format.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        pandas.DataFrame: DataFrame with normalized LinkedIn URLs.
                          Returns an empty DataFrame if the file is invalid or has no LinkedIn URLs.
    """
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"Error: File not found at {csv_file}")
        return pd.DataFrame()

    def normalize_linkedin_url(url):
        """
        Normalizes a LinkedIn URL to the format: https://linkedin.com/in/username
        """
        if isinstance(url, str):
            # Extract username (alphanumeric characters and hyphens)
            match = re.search(r"/in/([a-zA-Z0-9-]+)/?$", url)
            if match:
                username = match.group(1)
                return f"https://www.linkedin.com/in/{username}"
        return None

    # Apply the function to create a new 'normalized_url' column
    df['normalized_url'] = df['url'].apply(normalize_linkedin_url)

    # Filter out rows where 'normalized_url' is None (invalid URLs)
    df = df[df['normalized_url'].notna()]

    # Select only the 'title' and 'normalized_url' columns and rename
    if not df.empty:
        df = df[['title', 'normalized_url']].rename(columns={'normalized_url': 'url'})
    else:
        print("No valid LinkedIn URLs found in the file.")

    return df

# Example Usage:
csv_file_path = "linkedin_profiles.csv"  # Replace with the path to your CSV file
result_df = process_linkedin_data(csv_file_path)

if not result_df.empty:
    print(result_df)
    # Save to a new CSV (optional)
    result_df.to_csv("data/normalized_linkedin_data.csv", index=False)
else:
    print("No data to save.")

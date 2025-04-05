import pandas as pd

#cleans and processes subpage data from a df
def clean_subpages(subpages_df: pd.DataFrame):
    #elodes the "subpages" column into individual row
    exploded = subpages_df.explode("subpages").reset_index(drop=True)
    #flters out subpages containing "/#" to remove invalid links
    filtered = exploded[~exploded["subpages"].str.contains(r"/#", na=False)].copy()
    subpage_counts = (
        filtered
        .groupby(["id", "main_link"])["subpages"]
        .count()
        .reset_index(name="num_subpages")
    )
    
    filtered = (
        filtered
        .drop(columns="num_subpages", errors="ignore")
        .merge(subpage_counts, on=["id", "main_link"], how="left")
    )
    
    return filtered
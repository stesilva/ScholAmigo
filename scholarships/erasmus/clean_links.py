import pandas as pd

def clean_subpages(subpages_df: pd.DataFrame) -> pd.DataFrame:
    exploded = subpages_df.explode("subpages").reset_index(drop=True)
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


def filter_by_subpage_count(df: pd.DataFrame, min_count: int = 5, max_count: int = 15) -> pd.DataFrame:
    mask = (df["num_subpages"] >= min_count) & (df["num_subpages"] <= max_count)
    return df[mask].reset_index(drop=True)

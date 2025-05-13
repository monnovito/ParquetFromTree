import pandas as pd
import glob
from concurrent.futures import ThreadPoolExecutor
import os

def read_file(f):
    return pd.read_parquet(f)

if __name__ == "__main__":
    file_list = sorted(glob.glob("output_filtered/run_*.parquet"))
    print(f"Reading and merging {len(file_list)} filtered files...")

    with ThreadPoolExecutor(max_workers=8) as executor:
        dfs = list(executor.map(read_file, file_list))

    print("Concatenating...")
    df_all = pd.concat(dfs, ignore_index=True)

    # make Result directory
    
    if not os.path.exists("Result"):
        os.makedirs("Result")
    
    df_all.to_parquet("Result/combined_filtered.parquet")
    print(f"✅ Saved: Result/combined_filtered.parquet with {len(df_all)} rows")

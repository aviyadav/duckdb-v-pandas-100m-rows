import time, pandas as pd
from duckdb_bench import t

DATA = "data/data_100m.parquet"
DIM = "data/dim_100m.parquet"

# Use pyarrow backend for efficient columnar reads
pd.options.mode.copy_on_write = True

def read_scan():
    df = pd.read_parquet(DATA, engine='pyarrow')
    return len(df)

def filter_query():
    df = pd.read_parquet(DATA, engine='pyarrow')
    # Make category categorical to save memory and speed filters/group-bys
    df["category"] = df["category"].astype("category")
    return df[(df["category"].isin(["cat_1", "cat_2", "cat_3"])) & (df["value"] > 500)]
    
def groupby_query():
    df = pd.read_parquet(DATA, engine='pyarrow')
    df["category"] = df["category"].astype("category")
    return df.groupby("category", observed=True)["value"].agg(['mean', 'sum'])

def join_query():
    df = pd.read_parquet(DATA, engine='pyarrow')
    df["category"] = df["category"].astype("category")
    dim = pd.read_parquet(DIM, engine='pyarrow')
    dim["category"] = dim["category"].astype("category")

    merged = df.merge(dim, on="category", how="left")
    return merged.groupby("grp", observed=True)["value"].agg(['mean', 'sum'])

print("Pandas Benchmarking (s):")
print("scan:", round(t(read_scan), 3))
print("filter:", round(t(filter_query), 3))
print("groupby:", round(t(groupby_query), 3))
print("join:", round(t(join_query), 3))
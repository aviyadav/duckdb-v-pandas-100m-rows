import time, pandas as pd

DATA = "data/data_100m.parquet"
DIM = "data/dim_10k.parquet"

# Use pyarrow backend for efficient columnar reads
pd.options.mode.copy_on_write = True

def t(fn):
    fn()
    s=time.perf_counter(); fn(); e=time.perf_counter()
    return e - s

def read_scan():
    df = pd.read_parquet(DATA, engine='pyarrow')
    return len(df)

def filter_query():
    # Use column filtering to read only needed columns
    df = pd.read_parquet(DATA, engine='pyarrow', columns=['category', 'value'])
    # Make category categorical to save memory and speed filters/group-bys
    df["category"] = df["category"].astype("category")
    return df[(df["category"].isin(["cat_1", "cat_2", "cat_3"])) & (df["value"] > 500)]
    
def groupby_query():
    # Read only needed columns to reduce memory usage
    df = pd.read_parquet(DATA, engine='pyarrow', columns=['category', 'value'])
    df["category"] = df["category"].astype("category")
    return df.groupby("category", observed=True)["value"].agg(['mean', 'sum'])

def join_query():
    # Read only needed columns from main table
    df = pd.read_parquet(DATA, engine='pyarrow', columns=['category', 'value'])
    df["category"] = df["category"].astype("category")
    dim = pd.read_parquet(DIM, engine='pyarrow')
    dim["category"] = dim["category"].astype("category")
    dim["grp"] = dim["grp"].astype("category")

    # Use merge with categorical optimization
    merged = df.merge(dim, on="category", how="left")
    return merged.groupby("grp", observed=True)["value"].agg(['mean', 'sum'])

print("Pandas Benchmarking (s):")
print("scan:", round(t(read_scan), 3))
print("filter:", round(t(filter_query), 3))
print("groupby:", round(t(groupby_query), 3))
print("join:", round(t(join_query), 3))
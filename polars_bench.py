import time
import polars as pl

DATA = "data/data_100m.parquet"
DIM = "data/dim_10k.parquet"

def t(fn):
    fn()
    s = time.perf_counter()
    fn()
    e = time.perf_counter()
    return e - s

def read_scan():
    df = pl.scan_parquet(DATA).collect()
    return len(df)

def filter_query():
    df = pl.scan_parquet(DATA)
    return df.filter(
        (pl.col("category").is_in(["cat_1", "cat_2", "cat_3"])) & 
        (pl.col("value") > 500)
    ).collect()
    
def groupby_query():
    df = pl.scan_parquet(DATA)
    return df.group_by("category").agg([
        pl.col("value").mean().alias("mean"),
        pl.col("value").sum().alias("sum")
    ]).collect()

def join_query():
    # Use lazy evaluation for query optimization
    df = pl.scan_parquet(DATA)
    dim = pl.scan_parquet(DIM)
    
    # Polars will optimize the entire query plan before execution
    result = (
        df.join(dim, on="category", how="left")
        .group_by("grp")
        .agg([
            pl.col("value").mean().alias("mean"),
            pl.col("value").sum().alias("sum")
        ])
        .collect()  # Execute the optimized query plan
    )
    return result

print("Polars Benchmarking (s):")
print("scan:", round(t(read_scan), 3))
print("filter:", round(t(filter_query), 3))
print("groupby:", round(t(groupby_query), 3))
print("join:", round(t(join_query), 3))

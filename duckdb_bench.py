import time, duckdb
    
DATA = "data/data_100m.parquet"
DIM = "data/dim_10k.parquet"

con = duckdb.connect()
con.execute("PRAGMA threads=8")
con.execute("PRAGMA memory_limit='10GB'")

def t(fn):
    fn()
    s=time.perf_counter(); fn(); e=time.perf_counter()
    return e - s

def read_scan():
    con.execute(f"SELECT count(*) FROM '{DATA}';").fetchall()

def filter_query():
    con.execute(f"""
    SELECT * FROM '{DATA}'
    WHERE category IN ('cat_1', 'cat_2', 'cat_3') AND value > 500
    """).fetchall()

def groupby_query():
    con.execute(f"""
    SELECT category, AVG(value) as avg_val, SUM(value) as sum_val
    FROM '{DATA}'
    GROUP BY category
    """).fetchall()

def join_query():
    con.execute(f"""
    SELECT d.grp, AVG(f.value) as avg_val, SUM(f.value) as sum_val
    FROM '{DATA}' f
    JOIN '{DIM}' d ON f.category = d.category
    GROUP BY d.grp
    """).fetchall()

print("DuckDB timings (seconds):")
print("Scan: ", round(t(read_scan), 3))
print("Filter: ", round(t(filter_query), 3))
print("GroupBy: ", round(t(groupby_query), 3))
print("Join: ", round(t(join_query), 3))
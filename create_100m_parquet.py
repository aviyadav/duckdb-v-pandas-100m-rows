import duckdb, os, math, datetime

start = datetime.datetime.now()

N = 100_000_000 # 100M rows
OUTPUT_DIR = "data"
OUTPUT_FILE = OUTPUT_DIR + "/data_100m.parquet"
DIM_OUT = OUTPUT_DIR + "/dim_10k.parquet"

# create a directory for the parquet file
os.makedirs("data", exist_ok=True)

# create a connection to duckdb
con = duckdb.connect()
con.execute(f"PRAGMA threads={os.cpu_count()} - 1;") # use all available cores

# Synthetic fact table (vectorized inside DuckDB; no Python loop)
con.execute(f"""
COPY (
    SELECT 
        i:: BIGINT AS id,
        'cat_' || (i % 10000):: VARCHAR AS category, -- 100 categories
        (random() * 1000)::DOUBLE as value,
        TIMESTAMP '2022-01-01' + (i % 1_000_000) * INTERVAL '1 second' AS ts
    FROM range({N}) AS t(i)
) TO '{OUTPUT_FILE}' (FORMAT 'parquet', COMPRESSION 'ZSTD');
""")

print(f"Time taken for data creation: {datetime.datetime.now() - start}")

# Small dimension table for join (10k rows)
con.execute(f"""
COPY (
  SELECT 
    'cat_' || i::VARCHAR AS category,
    'group_' || (i % 10)::VARCHAR AS grp
  FROM range(10000) t(i)
) TO '{DIM_OUT}' (FORMAT PARQUET, COMPRESSION ZSTD);
""")

print(f"Time taken for data and dimension creation: {datetime.datetime.now() - start}")
# duckdb-v-pandas-100m-rows


Results of benchmark for 100m rows (in sec)

AMD Ryzen 7 (16GB)

| Framework | Scan   | Filter | GroupBy  | Join   |
| --------- | -----  | ------ | -------- | -----  |
| DuckDB    | 0.011  | 3.76   | 1.236    | 2.093  |
| Pandas    | 10.036 | 11.441 | 14.363   | 26.831 | 
| Polars    | 2.81   | 2.148  | 10.295   | 6.781  |

Intel core i9 (32 GB)

| Framework | Scan   | Filter | GroupBy  | Join   |
| --------- | -----  | ------ | -------- | -----  |
| DuckDB    | 0.005  | 0.971  | 0.494    | 0.802  |
| Pandas    | 4.237  | 9.31   | 11.328   | 21.657 | 
| Polars    | 0.918  | 0.884  | 4.948    | 2.471  |



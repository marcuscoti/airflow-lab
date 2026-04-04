import duckdb
#duckdb.read_parquet("airflow-container\\data\\sales\\batch_20260402193137.parquet")
print(duckdb.sql("SELECT _insert_timestamp, COUNT(*) " \
"FROM 'airflow-container\\data\\sales\\*.parquet'" \
"GROUP BY _insert_timestamp"
))
#SELECT MAX(data_atualizacao)  FROM 'airflow-container\\data\\sales\\*.parquet';
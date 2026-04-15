# airflow-lab
Airflow solution to ingest data from a Postgre database, save locally and sync with Azure Datalake gen2.

## Data
The data is from a live database, that simulates a car company, containing sales, locations, customers and vehicles.
Every day, we have new sales data available.

Source extracted from Fernando Amaral course: https://www.eia.ai/bootcamp-engenharia-de-dados-construa-um-projeto-real


## Configuration
Docker container - see docker-compose.yaml

## DAGs
We have two DAG types:
- raw: these DAGs copies the data from the database and saves as parquet files locally.
- adls: syncs the parquet files in the Azure Datalake.

## Full VS Incremental Loads
All tables are using full loads, except the sales data, where we have the incremental logic in place.

## Plugins
We developed a ADLS plugin to handle basic operations on the Datalake, feel free to customize.

## Next Steps
- Create a plugin to reuse the operators for raw_ DAGs.
- Improve the entire dataflow, syncing the data directly into cloud, avoiding to save locally.


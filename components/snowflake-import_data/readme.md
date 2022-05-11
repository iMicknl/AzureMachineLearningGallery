# Import Snowflake Data


## How to use it

(Document Keyvault usage)

## Local Development

```cd components/snowflake-import_data/```

```conda env create -f snowflake_import_data_conda.yaml --force```

```conda activate snowflake_import_data_component_environment``

```python3 import_data.py --query="SELECT * FROM t" --host="test.snowflakecomputing.com" --user="test" --password="test" --account="test" --warehouse="" --database="" --schema="" --protocol="https" --port="443"```
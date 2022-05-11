import argparse
import snowflake.connector

from azureml.core import Run
from azureml.studio.core.io.data_frame_directory import save_data_frame_to_directory
from azureml.studio.core.data_frame_schema import DataFrameSchema
from azureml.studio.core.logger import module_logger as logger

def main():
    logger.debug("Initializing Snowflake Import Data component ...")

    # See all possible arguments in xxxx
    # or by passing the --help flag to this script.
    # We now keep distinct sets of args, for a cleaner separation of concerns.
    parser = argparse.ArgumentParser()

    # Input
    parser.add_argument('--query', type=str, help='')

    parser.add_argument('--host', type=str, help='')
    parser.add_argument('--user', type=str, help='')
    parser.add_argument('--password', type=str, help='')
    parser.add_argument('--account', type=str, help='')
    parser.add_argument('--warehouse', type=str, help='')
    parser.add_argument('--database', type=str, help='')
    parser.add_argument('--schema', type=str, help='')
    parser.add_argument('--protocol', type=str, help='')

    # Output
    parser.add_argument('--results_dataset', type=str, help='dataframe')

    args = parser.parse_args()

    # Retrieve secrets via code in submitted run
    run = Run.get_context()
    secrets = [args.host, args.user, args.password, args.account,  args.warehouse, args.database, args.schema, args.protocol, args.port]
    host, user, password, account, warehouse, database, schema, protocol, port = run.get_secrets(secrets)

    ctx = snowflake.connector.connect(
            host=host,
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            protocol=protocol,
            port=port)

    # Create a cursor object.
    cur = ctx.cursor()

    # Execute a statement that will generate a result set.
    sql = args.query or "SELECT * FROM t"
    
    try:
        cur.execute(sql) 
        df = cur.fetch_pandas_all() # consider using cur.fetch_pandas_batches()
    finally:
        # Always close connections if there is an error
        cur.close()

    # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
    save_data_frame_to_directory(save_to=args.results_dataset,
                                 data=df,
                                 schema=DataFrameSchema.data_frame_to_dict(df))

    ctx.close()

if __name__ == '__main__':
    main()

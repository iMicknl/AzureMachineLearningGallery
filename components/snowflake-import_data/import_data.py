""" Import Snowflake Data"""
import argparse
import os
import snowflake.connector

from azureml.core import Run
from azureml.studio.core.io.data_frame_directory import save_data_frame_to_directory
from azureml.studio.core.data_frame_schema import DataFrameSchema
from azureml.studio.core.logger import module_logger as logger
from azureml.core.run import _OfflineRun

def main():
    """ Main """
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
    parser.add_argument('--port', type=str, help='')

    # Output
    parser.add_argument('--output-dir', type=str, help='dataframe')

    args = parser.parse_args()
    input_args = [args.host, args.user, args.password, args.account,  args.warehouse, args.database, args.schema, args.protocol, args.port]

    # Retrieve secrets via code in submitted run
    run = Run.get_context()

    if isinstance(run, _OfflineRun):
        # Retrieve values from arguments
        logger.debug("Retrieving values from passed arguments") 
        host, user, password, account, warehouse, database, schema, protocol, port = input_args
    else:
        # Retrieve values from KeyVault
        logger.debug("Retrieving values from KeyVault") 

        # TODO rewrite to use run.get_secrets(input_args) in batch
        host = run.get_secret(args.host)
        user = run.get_secret(args.user)
        password = run.get_secret(args.password)
        account = run.get_secret(args.account)
        warehouse = run.get_secret(args.warehouse)
        database = run.get_secret(args.database)
        schema = run.get_secret(args.schema)
        protocol = args.protocol
        port = args.port
    
    ctx = snowflake.connector.connect(
            host=host,
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            protocol=protocol,
            port=port
    )

    # Create a cursor object.
    cur = ctx.cursor()

    # Execute a statement that will generate a result set.
    sql = args.query or "SELECT * FROM t"
    
    try:
        cur.execute(sql) 
        snowflake_df = cur.fetch_pandas_all() # consider using cur.fetch_pandas_batches()
    finally:
        # Always close connections if there is an error
        cur.close()

    # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
    os.makedirs(args.output_dir, exist_ok=True)
    save_data_frame_to_directory(save_to=args.output_dir,
                                 data=snowflake_df,
                                 schema=DataFrameSchema.data_frame_to_dict(snowflake_df))

    ctx.close()

if __name__ == '__main__':
    main()

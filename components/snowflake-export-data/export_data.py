""" Export Snowflake Data"""
import argparse
import snowflake.connector

from azureml.core import Run
from azureml.studio.core.data_frame_schema import DataFrameSchema
from azureml.studio.core.logger import module_logger as logger
from azureml.core.run import _OfflineRun

from snowflake.connector.pandas_tools import write_pandas
from azureml.studio.core.io.data_frame_directory import load_data_frame_from_directory



def main():
    """ Main """
    logger.debug("Initializing Snowflake Import Data component ...")

    # See all possible arguments in xxxx
    # or by passing the --help flag to this script.
    # We now keep distinct sets of args, for a cleaner separation of concerns.
    parser = argparse.ArgumentParser()

    # Input
    parser.add_argument('--input-dir', type=str, help='dataframe')
    parser.add_argument('--table', type=str, help='')

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
    # none?

    args = parser.parse_args()
    input_args = [args.host, args.user, args.password, args.account,  args.warehouse, args.database, args.schema, args.protocol, args.port]
    logger.debug("Received the following input: ", input_args)

    # Retrieve secrets via code in submitted run
    run = Run.get_context()

    if isinstance(run, _OfflineRun):
        # Retrieve values from arguments
        logger.debug("Retrieving values from passed arguments") 
        host, user, password, account, warehouse, database, schema, protocol, port = input_args
    else:
        # Retrieve values from KeyVault
        logger.debug("Retrieving values from KeyVault") 

        # TODO rewrite to use run.get_secrets(input_args)
        host = run.get_secret(args.host)
        user = run.get_secret(args.host)
        password = run.get_secret(args.host)
        account = run.get_secret(args.host)
        warehouse = run.get_secret(args.host)
        database = run.get_secret(args.host)
        schema = run.get_secret(args.host)
        protocol = args.protocol
        port = args.port


    # Create a DataFrame containing data about customers
    df = load_data_frame_from_directory(args.input_dir).data

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

    try:
        # Write the data from the DataFrame to the table named "customers".
        success, nchunks, nrows, _ = write_pandas(ctx, df, args.table) 
        logger.debug(success, nchunks, nrows)

    finally:
        # Always close connections if there is an error
        ctx.close()

    
if __name__ == '__main__':
    main()

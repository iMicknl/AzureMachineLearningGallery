

# pip install snowflake-connector-python[secure-local-storage,pandas]

# Code in submitted run
from azureml.core import Experiment, Run

run = Run.get_context()
secret_value = run.get_secret(name="mysecret")


ctx = snowflake.connector.connect(
          host=host,
          user=user,
          password=password,
          account=account,
          warehouse=warehouse,
          database=database,
          schema=schema,
          protocol='https',
          port=port)
# Create a cursor object.
cur = ctx.cursor()
# Execute a statement that will generate a result set.
sql = "select * from t"
cur.execute(sql)
# Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
df = cur.fetch_pandas_all()
# ...
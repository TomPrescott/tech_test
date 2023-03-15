import snowflake.connector
import sys
from load_csv_to_snowflake import load_csvs_to_snowflake_table 

def main():

    #Set your Snowflake connection details
    account = 'YOUR ACCOUNT'
    user = 'YOUR USERNAME'
    password = 'YOUR PASSWORD'

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
    except Exception as e:
        print('Error connecting to snowflake')
        sys.exit(1)

    #Set your csv files to be loaded e.g. csv_file_paths = ['/home/username/data/file1.csv', '/home/username/data/file2.csv']
    csv_file_paths = []

    #Add your fully qualified table name here e.g: fully_qualified_table_name = "BROOKLYN_DATA.FINANCE.TRANSACTIONS"
    fully_qualified_table_name = 'YOUR_DATABASE.YOUR_SCHEMA.YOUR_TABLE'
    load_csvs_to_snowflake_table(conn, fully_qualified_table_name, csv_file_paths)

if __name__ == "__main__":
    main()

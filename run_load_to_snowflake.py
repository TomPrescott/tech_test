import sys
import glob
import os
import snowflake.connector
from load_csv_to_snowflake import load_csvs_to_snowflake_table 

def main():

    #Set your Snowflake connection details
    account = "YOUR ACCOUNT"
    user = "YOUR USERNAME"
    password = "YOUR PASSWORD"

    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
    except Exception as e:
        print("Error connecting to snowflake")
        sys.exit(1)

    #Set your directory containing csv file to load into Snowflake e.g. "/home/user/data/csv_files"
    csv_dir = ""

    try:
        csv_file_paths = glob.glob(os.path.join(csv_dir, "*.csv"))
    except Exception as e:
        print("Error loading csv files")
        sys.exit(1)

    #Add your fully qualified table name here e.g. fully_qualified_table_name = "BROOKLYN_DATA.FINANCE.TRANSACTIONS"
    fully_qualified_table_name = "YOUR_DATABASE.YOUR_SCHEMA.YOUR_TABLE"
    load_csvs_to_snowflake_table(conn, fully_qualified_table_name, csv_file_paths)

if __name__ == "__main__":
    main()

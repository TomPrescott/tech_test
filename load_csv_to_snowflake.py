import os
import json
import pandas as pd 
import snowflake.connector

def load_csvs_to_snowflake_table(
    conn: snowflake.connector.connection.SnowflakeConnection,
    fully_qualified_table_name: str,
    csv_file_paths: list[str]
):

    cur = conn.cursor()

    # Get column names from target table schema
    cur.execute(f"SELECT * FROM {fully_qualified_table_name} WHERE 1=0;")
    column_names = [col[0] for col in cur.description]
    column_names = [item.upper() for item in column_names]

    for csv_file_path in csv_file_paths:
        try: 
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(csv_file_path)
            df.columns = [col.upper() for col in df.columns]
            
            # Reorder the columns and save the reordered DataFrame to a new CSV file
            df = df[column_names]
            temp_csv_file_path = csv_file_path[:-4] + "_reordered.csv"
            df.to_csv(temp_csv_file_path, index=False)
    
            # Create Stage
            stage = fully_qualified_table_name + "_STAGE"
            create_staging_query = f"create or replace stage {stage} file_format = (TYPE=CSV);"
            cur.execute(create_staging_query)
    
            # Upload file from local to staging table
            put_query = f"put file://{temp_csv_file_path} @{stage} auto_compress=true"
            cur.execute(put_query)
    
            # Upload file from staging to final table
            filename = os.path.basename(temp_csv_file_path)
            copy_query = f"copy into {fully_qualified_table_name} from @{stage}/{filename}.gz file_format = (TYPE=CSV, SKIP_HEADER=1);"
            cur.execute(copy_query)
    
            # Remove the temporary reordered CSV file
            os.remove(temp_csv_file_path)
            print(f"{csv_file_path} successfully written to {fully_qualified_table_name}")
            
        except Exception as e:
            print(f"Error writing {csv_file_path} to Snowflake: {e}")


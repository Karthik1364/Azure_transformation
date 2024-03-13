import json
from databricks import sql
import pandas as pd

def getDataForecasting():
    print("connecting to DB")
    connection = sql.connect(
        server_hostname="adb-8677847876952736.16.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/d71cacee8a898c0b",
        access_token="dapib578b4eb1ffc581bd6721ed244cbe610-3"
    )
    # SQL query
    query = "select _airbyte_data  from `hive_metastore`.`default`.`_airbyte_raw_salesforecast`;"
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    # Convert JSON strings to dictionaries and exclude 'uri' metadata
    result_data = []
    for row in result:
        row_dict = json.loads(row[0])['d']['results']
        filtered_row = [{k: v for k, v in item.items() if k != '__metadata'} for item in row_dict]
        result_data.extend(filtered_row)
    
    # Convert the data to a DataFrame
    result_df = pd.DataFrame(result_data)

    print(result_df)

    return result_df

def main():
    result_df = getDataForecasting()
    print(result_df)

if __name__ == "__main__":
    main()

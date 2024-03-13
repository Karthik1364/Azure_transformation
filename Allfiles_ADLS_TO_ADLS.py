import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# Function to flatten nested JSON
def flatten_json(json_data, parent_key='', sep='_'):
    items = {}
    for k, v in json_data.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_json(v, new_key, sep=sep).items())
        else:
            items[new_key] = v
    return items

# Enter credentials
account_name = 'rgdatalakecsvfile'
account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='
container_name = 'airbytecontainer'
folder_name = 'Employee'

# Create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

# Get a list of all blob files in the 'Employee' folder of the container
blob_list = [blob.name for blob in container_client.list_blobs(name_starts_with=folder_name)]

df_list = []

# Generate a shared access signature for files and load them into Python
for blob_i in blob_list:
    try:
        # Generate a shared access signature for each blob file
        sas_i = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_i,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )

        # Properly encode the blob name before constructing the URL
        encoded_blob_name = quote(blob_i)

        sas_url = 'https://' + account_name + '.blob.core.windows.net/' + container_name + '/' + encoded_blob_name + '?' + sas_i

        # Read files based on their extensions
        if blob_i.endswith('.csv'):
            df = pd.read_csv(sas_url)
            df_list.append(df)
        elif blob_i.endswith('.json'):
            with container_client.get_blob_client(blob_i) as blob_client:
                json_data = blob_client.download_blob().readall()
                json_dict = json.loads(json_data)
                if isinstance(json_dict, list) and json_dict:
                    # If JSON is a list of dictionaries, flatten and convert to DataFrame
                    flattened_data = [flatten_json(item) for item in json_dict]
                    df_json = pd.DataFrame(flattened_data)
                    df_list.append(df_json)
                    print(f"Successfully read and transformed JSON file: {blob_i}")
                elif isinstance(json_dict, dict) and json_dict:
                    # If JSON is a dictionary, flatten and convert to DataFrame
                    flattened_data = flatten_json(json_dict)
                    df_json = pd.DataFrame([flattened_data])
                    df_list.append(df_json)
                    print(df_list)
                    print(f"Successfully read and transformed JSON file: {blob_i}")
                else:
                    print(f"Invalid JSON format or empty JSON file: {blob_i}")
    except Exception as e:
        print(f"Error processing blob {blob_i}: {str(e)}")

# Define the output container name
output_container_name = 'outputcontainer'

# Create a client to interact with blob storage for output
output_container_client = blob_service_client.get_container_client(output_container_name)

# Create the output container if it doesn't exist
if not output_container_client.exists():
    output_container_client.create_container()

# Upload data frames to the output container
for idx, df in enumerate(df_list):
    try:
        # Generate a unique name for the output blob
        output_blob_name = f"output_{idx}.csv"
        
        # Convert DataFrame to CSV
        df_csv = df.to_csv(index=False)
        
        # Upload the CSV data to the output blob
        output_blob_client = output_container_client.get_blob_client(output_blob_name)
        output_blob_client.upload_blob(df_csv, overwrite=True)
        
        print(f"Successfully uploaded {output_blob_name} to the output container.")
    except Exception as e:
        print(f"Error uploading {output_blob_name}: {str(e)}")

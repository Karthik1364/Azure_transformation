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

# Specify the blob name you want to process
blob_name = 'Employee/bestrun.json'  # Include the folder name

# Create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

try:
    # Generate a shared access signature for the specified blob file
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=1)
    )

    # Properly encode the blob name before constructing the URL
    encoded_blob_name = quote(blob_name)

    sas_url = 'https://' + account_name + '.blob.core.windows.net/' + container_name + '/' + encoded_blob_name + '?' + sas

    # Read the specified file based on its extension
    if blob_name.endswith('.csv'):
        df = pd.read_csv(sas_url)
        print(df)
    elif blob_name.endswith('.json'):
        with container_client.get_blob_client(blob_name) as blob_client:
            json_data = blob_client.download_blob().readall()
            json_dict = json.loads(json_data)
            if isinstance(json_dict, list) and json_dict:
                # If JSON is a list of dictionaries, flatten and convert to DataFrame
                flattened_data = [flatten_json(item) for item in json_dict]
                df = pd.DataFrame(flattened_data)
                print(df)
                print(f"Successfully read and transformed JSON file: {blob_name}")
            elif isinstance(json_dict, dict) and json_dict:
                # If JSON is a dictionary, flatten and convert to DataFrame
                flattened_data = flatten_json(json_dict)
                df = pd.DataFrame([flattened_data])
                print(df)
                print(f"Successfully read and transformed JSON file: {blob_name}")
            else:
                print(f"Invalid JSON format or empty JSON file: {blob_name}")

    # Add code to store the processed DataFrame back into Azure Blob Storage
    # Define the output container name
    output_container_name = 'outputcontainer'

    # Create a client to interact with blob storage for output
    output_container_client = blob_service_client.get_container_client(output_container_name)

    # Create the output container if it doesn't exist
    if not output_container_client.exists():
        output_container_client.create_container()

    # Generate a unique name for the output blob
    output_blob_name = 'bestrun.csv'  # You can change the output file name as needed

    # Convert DataFrame to CSV
    df.to_csv(output_blob_name, index=False)

    # Upload the CSV data to the output blob
    with open(output_blob_name, "rb") as data:
        output_blob_client = output_container_client.get_blob_client(output_blob_name)
        output_blob_client.upload_blob(data, overwrite=True)

    print(f"Successfully uploaded {output_blob_name} to the output container.")

except Exception as e:
    print(f"Error processing or uploading blob {blob_name}: {str(e)}")

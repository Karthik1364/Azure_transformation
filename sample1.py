import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone
import urllib.request

def getDataForecasting():
    # Replace placeholders with your Azure Storage account name and account key
    account_name = 'rgdatalakecsvfile'
    account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='

    # Create a client to interact with blob storage
    connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Specify the container name and folder name
    container_name = 'airbytecontainer'
    folder_name = 'Employee'

    # Use the client to connect to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Specify the blob name you want to process
    blob_name = f'{folder_name}/new6.json'

    try:
        # Generate a shared access signature for the specified blob file
        sas = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )

        # Construct the URL with SAS token
        blob_url_with_sas = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}"

        # Read the JSON data from the blob
        with urllib.request.urlopen(blob_url_with_sas) as response:
            json_data = response.read().decode('utf-8')  # Ensure the data is decoded as UTF-8

        # Parse the JSON data
        data = json.loads(json_data)
        
        if isinstance(data, list):
            results_data = data  # Assume data is already a list of dictionaries
        elif isinstance(data, dict):
            results_data = data.get('d', {}).get('results', [])  # Extract 'results' if it's a dictionary
        
        # Convert the data to a DataFrame
        result_df = pd.DataFrame(results_data)
        
        # Drop the '__metadata' column if it exists
        if '__metadata' in result_df.columns:
            result_df.drop(columns=['__metadata'], inplace=True)

        return result_df

    except Exception as e:
        print(f"Error fetching or processing blob {blob_name}: {str(e)}")
        return None

def main():
    result_df = getDataForecasting()
    if result_df is not None:
        print(result_df)

if __name__ == "__main__":
    main()

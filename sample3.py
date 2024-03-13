import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone
import urllib.request

def getDataForecasting(blob_service_client, blob_name):
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

        # Read the data from the blob
        with urllib.request.urlopen(blob_url_with_sas) as response:
            if blob_name.endswith('.json'):
                data = json.load(response)
            elif blob_name.endswith('.txt'):
                data = response.read().decode('utf-8')
            else:
                raise ValueError("Unsupported file format")

        return data

    except Exception as e:
        print(f"Error fetching or processing blob {blob_name}: {str(e)}")
        return None

def main():
    # Replace placeholders with your Azure Storage account name and account key
    account_name = 'rgdatalakecsvfile'
    account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='

    # Create a client to interact with blob storage
    connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Specify the container name and folder name
    container_name = 'airbytecontainer'
    folder_name = 'Employee'

    # List of blob names to process
    blob_names = [f'{folder_name}/Sales_Custom.json', f'{folder_name}/Customer_response.txt']

    for blob_name in blob_names:
        data = getDataForecasting(blob_service_client, blob_name)
        if data is not None:
            if isinstance(data, dict) or isinstance(data, list):
                result_df = pd.DataFrame(data)
                # Drop the '__metadata' column if it exists
                if '__metadata' in result_df.columns:
                    result_df.drop(columns=['__metadata'], inplace=True)
                print(result_df)
            elif isinstance(data, str):
                print(data)  # Print text data
            else:
                print("Unsupported data type")
            print(f"Successfully processed {blob_name}.")

if __name__ == "__main__":
    main()

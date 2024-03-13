import pandas as pd
import json
import urllib.request
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone

def get_blob_data(blob_service_client, container_name, folder_name, blob_name):
    try:
        sas = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        blob_url_with_sas = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas}"
        with urllib.request.urlopen(blob_url_with_sas) as response:
            json_data = response.read().decode('utf-8')
        data = json.loads(json_data)
        results_data = data if isinstance(data, list) else data.get('d', {}).get('results', [])
        result_df = pd.DataFrame(results_data)
        if '__metadata' in result_df.columns:
            result_df.drop(columns=['__metadata'], inplace=True)
        return result_df, blob_name
    except Exception as e:
        print(f"Error fetching or processing blob {blob_name}: {str(e)}")
        return None, None

def upload_blob(blob_client, data, blob_name):
    try:
        with open(data, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Successfully uploaded {blob_name}.")
    except Exception as e:
        print(f"Error uploading blob {blob_name}: {str(e)}")

def main():
    account_name = 'rgdatalakecsvfile'
    account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='
    connect_str = f'DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_name = 'airbytecontainer'
    folder_name = 'Employee'
    blob_name = f'{folder_name}/new6.json'

    result_df, blob_name = get_blob_data(blob_service_client, container_name, folder_name, blob_name)
    
    if result_df is not None:
        print(result_df)
        try:
            output_container_name = 'outputcontainer'
            output_container_client = blob_service_client.get_container_client(output_container_name)
            if not output_container_client.exists():
                output_container_client.create_container()
            output_blob_name = 'new6.csv'
            result_df.to_csv(output_blob_name, index=False)
            output_blob_client = output_container_client.get_blob_client(output_blob_name)
            upload_blob(output_blob_client, output_blob_name, output_blob_name)
        except Exception as e:
            print(f"Error processing or uploading blob {blob_name}: {str(e)}")

if __name__ == "__main__":
    main()

import pandas as pd
import json
import urllib.request
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime, timedelta, timezone

class AzureBlobManager:
    def __init__(self, account_name, account_key, container_name):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.connect_str = f'DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net'
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)

    def get_blob_data(self, folder_name, blob_name):
        try:
            sas = self.generate_blob_sas(blob_name)
            blob_url_with_sas = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{blob_name}?{sas}"
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

    def generate_blob_sas(self, blob_name):
        return generate_blob_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            blob_name=blob_name,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=1)
        )

    def upload_blob(self, data, blob_name, output_container_name):
        try:
            with open(data, "rb") as data:
                blob_client = self.blob_service_client.get_blob_client(container=output_container_name, blob=blob_name)
                blob_client.upload_blob(data, overwrite=True)
            print(f"Successfully uploaded {blob_name}.")
        except Exception as e:
            print(f"Error uploading blob {blob_name}: {str(e)}")

def process_blob_data(blob_manager, folder_name, blob_name, output_container_name):
    result_df, blob_name = blob_manager.get_blob_data(folder_name, blob_name)
    if result_df is not None:
        print(result_df)
        try:
            output_blob_name = 'csvjson.csv'
            result_df.to_csv(output_blob_name, index=False)
            blob_manager.upload_blob(output_blob_name, output_blob_name, output_container_name)
        except Exception as e:
            print(f"Error processing or uploading blob {blob_name}: {str(e)}")

def main():
    account_name = 'rgdatalakecsvfile'
    account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='
    container_name = 'inputcontainer'
    folder_name = ''
    blob_name = f'{folder_name}/csvjson.json'
    output_container_name = 'outputcontainer'

    blob_manager = AzureBlobManager(account_name, account_key, container_name)
    process_blob_data(blob_manager, folder_name, blob_name, output_container_name)

if __name__ == "__main__":
    main()

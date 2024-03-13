from azure.storage.blob import BlobServiceClient, BlobType

def convert_append_blob_to_block_blob(account_name, account_key, container_name, append_blob_name, block_blob_name):
    # Connect to the Blob Service
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net",
                                            credential=account_key)

    # Get a reference to the append blob
    append_blob_client = blob_service_client.get_blob_client(container=container_name, blob=append_blob_name)

    # Get content from the append blob
    append_blob_content = append_blob_client.download_blob().readall()

    # Create a new block blob
    block_blob_client = blob_service_client.get_blob_client(container=container_name, blob=block_blob_name)
    
    # Upload content to the block blob
    block_blob_client.upload_blob(append_blob_content, blob_type=BlobType.BlockBlob)

    print(f"Append blob '{append_blob_name}' has been converted to block blob '{block_blob_name}'.")

# Replace these with your Azure Storage account credentials and blob names
account_name = 'rgdatalakecsvfile'
account_key = 'bMMgHBH+99srspNJsHpM/K9+Co9nTfffv3Hair7dVU7IjGOp1FkeZR8XEWSvDSIor2go37Y57Uma+ASti1FNcQ=='
container_name = 'airbytecontainer'
append_blob_name = '2024_03_04_1709535837732_0'
block_blob_name = "updatedAB"

convert_append_blob_to_block_blob(account_name, account_key, container_name, append_blob_name, block_blob_name)

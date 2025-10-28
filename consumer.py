import argparse
import json
import boto3
from typing import Optional, Dict, Any

parser = argparse.ArgumentParser(description="A consumer program to interact with producers.")
parser.add_argument("resources", type=str, help="Resources.")
parser.add_argument("storage", type=str, help="Storage strategy.")
args = parser.parse_args()

def main():
    resources = args.resources
    request_bucket = 'usu-cs5270-orangutan-requests'
    storage = args.storage
    storage_bucket = 'usu-cs5270-orangutan-web'
    client = boto3.client('s3')

    key = get_next_key_from_bucket(client, request_bucket)
    if key:
        print(f"Processing request file: {key}")
        file_content = get_file_from_s3(client, request_bucket, key)
        process_one_request(client, file_content, storage_bucket)
        delete_file_from_s3(client, request_bucket, key)

    print(f"Consuming resources: {resources} with storage strategy: {storage}")

def get_next_key_from_bucket(s3_client: boto3.client, bucket_name: str) -> Optional[str]:
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    if response.get('Contents'):
        s3_key = response['Contents'][0]['Key']
        return s3_key
    else:
        return None

def get_file_from_s3(s3_client: boto3.client, bucket_name: str, s3_key: str) -> str:
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def delete_file_from_s3(s3_client: boto3.client, bucket_name: str, s3_key: str):
    s3_client.delete_object(Bucket=bucket_name, Key=s3_key)

def handle_update_request(parsed_request, storage_bucket):
    raise NotImplementedError

def handle_delete_request(parsed_request, storage_bucket):
    raise NotImplementedError

def handle_create_request(s3_client: boto3.client, parsed_request, storage_bucket):
    widget = make_widget_from_request(parsed_request)
    widget_json = json.dumps(widget)
    s3_client.put_object(
            Bucket=storage_bucket,
            Key=f"widgets/{widget['id']}",
            Body=widget_json,
            ContentType='text')

def make_widget_from_request(parsed_request: Dict[str, Any]) -> Dict[str, Any]:
    # Create a new dictionary to hold the transformed data
    widget = {}
    for key, value in parsed_request.items():
        if key == 'type' or key == 'requestId':
            # Skip these keys (remove them)
            continue
        elif key == 'widgetId':
            # Rename 'widgetId' to 'id'
            widget['id'] = value
        else:
            # Copy all other fields as is
            widget[key] = value
    return widget


def process_one_request(s3_client: boto3.client, json_string: str, storage_bucket: str):
    parsed_request = json.loads(json_string)
    if parsed_request["type"] == "create":
        handle_create_request(s3_client, parsed_request, storage_bucket)
    elif parsed_request["type"] == "delete":
        handle_delete_request(parsed_request, storage_bucket)
    elif parsed_request["type"] == "update":
        handle_update_request(parsed_request, storage_bucket)
        

    print(type(parsed_request))
    print(parsed_request["type"])
    print(parsed_request["label"])
    print(parsed_request["otherAttributes"][0]["name"])

if __name__ == "__main__":
    main()
import argparse
import json
import boto3
from typing import Optional

parser = argparse.ArgumentParser(description="A consumer program to interact with producers.")
parser.add_argument("resources", type=str, help="Resources.")
parser.add_argument("storage", type=str, help="Storage strategy.")
args = parser.parse_args()

def main():
    resources = args.resources
    storage = args.storage
    key = get_next_key_from_bucket(boto3.client('s3'), 'usu-cs5270-orangutan-requests')
    if key:
        file_content = get_file_from_s3(boto3.client('s3'), 'usu-cs5270-orangutan-requests', key)
        read_one_request(file_content)
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

def read_one_request(json_string: str):
    parsed_request = json.loads(json_string)
    print(type(parsed_request))
    print(parsed_request["type"])
    print(parsed_request["label"])
    print(parsed_request["otherAttributes"][0]["name"])

if __name__ == "__main__":
    main()
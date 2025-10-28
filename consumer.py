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
    file = get_next_file_from_bucket(boto3.client('s3'), 'usu-cs5270-orangutan-requests')
    print(f"Consuming resources: {resources} with storage strategy: {storage}")
    print(f"Next file to process: {file}")

def get_next_file_from_bucket(s3_client: boto3.client, bucket_name: str) -> Optional[str]:
    list_args = {'Bucket': bucket_name, 'MaxKeys': 1}
        
    response = s3_client.list_objects_v2(**list_args)

    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    # Check if any objects were returned
    if response.get('Contents'):
        first_object = response['Contents'][0]
        s3_key = first_object['Key']
        print(f"Found next file key to process: {s3_key}")
        return s3_key
    else:
        print(f"Bucket '{bucket_name}' is empty.")
        return None

def read_one_request(json_string: str):
    parsed_request = json.loads(json_string)
    print(type(parsed_request))
    print(parsed_request["type"])
    print(parsed_request["label"])
    print(parsed_request["otherAttributes"][0]["name"])

if __name__ == "__main__":
    main()
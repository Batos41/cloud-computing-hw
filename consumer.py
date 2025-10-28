import argparse
import json
import boto3
import logging
import time
from typing import Optional, Dict, Any

parser = argparse.ArgumentParser(description="A consumer program to interact with producers.")
parser.add_argument("resources", type=str, help="Resources.")
parser.add_argument("storage", type=str, help="Storage strategy.")
# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

client = boto3.client('s3')
args = parser.parse_args()

def main():
    resources = args.resources
    request_bucket = 'usu-cs5270-orangutan-requests'
    storage = args.storage
    storage_bucket = 'usu-cs5270-orangutan-web'

    key = get_next_key_from_bucket(request_bucket)
    #while True:
    if key:
        logger.info(f"Processing request file: {key}")
        file_content = get_file_from_s3(request_bucket, key)
        process_one_request(file_content, storage_bucket)
        delete_file_from_s3(request_bucket, key)
    else:
        # wait 100 ms before checking again
        time.sleep(0.1)

    #logger.info(f"Consuming resources: {resources} with storage strategy: {storage}")

def get_next_key_from_bucket(bucket_name: str) -> Optional[str]:
    response = client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    if response.get('Contents'):
        s3_key = response['Contents'][0]['Key']
        return s3_key
    else:
        return None

def get_file_from_s3(bucket_name: str, s3_key: str) -> str:
    response = client.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def delete_file_from_s3(bucket_name: str, s3_key: str):
    client.delete_object(Bucket=bucket_name, Key=s3_key)

def handle_update_request(parsed_request, storage_bucket):
    raise NotImplementedError

def handle_delete_request(parsed_request, storage_bucket):
    raise NotImplementedError

def handle_create_request(parsed_request, storage_bucket):
    logger.info(f"Process create request for widget {parsed_request['widgetId']} in request {parsed_request['requestId']}")
    widget = make_widget_from_request(parsed_request)
    widget_json = json.dumps(widget)
    owner = kebab_owner(parsed_request.get("owner"))
    key = f"widgets/{owner}/{widget['id']}"
    logger.info(f"Add to s3 bucket {storage_bucket} a widget with key = {key}")
    client.put_object(
            Bucket=storage_bucket,
            Key=key,
            Body=widget_json,
            ContentType='application/json')

def make_widget_from_request(parsed_request: Dict[str, Any]) -> Dict[str, Any]:
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

def kebab_owner(owner: str) -> str:
    return owner.lower().replace(" ", "-")

def process_one_request(json_string: str, storage_bucket: str):
    parsed_request = json.loads(json_string)
    if parsed_request["type"] == "create":
        handle_create_request(parsed_request, storage_bucket)
    elif parsed_request["type"] == "delete":
        handle_delete_request(parsed_request, storage_bucket)
    elif parsed_request["type"] == "update":
        handle_update_request(parsed_request, storage_bucket)
        

    # debug useful request internals
    logger.debug("parsed_request type: %s", type(parsed_request))
    logger.debug("request type: %s", parsed_request.get("type"))
    logger.debug("label: %s", parsed_request.get("label"))
    # show first otherAttribute name if present
    if parsed_request.get("otherAttributes"):
        first_attr = parsed_request["otherAttributes"][0]
        logger.debug("first otherAttribute name: %s", first_attr.get("name"))

if __name__ == "__main__":
    main()
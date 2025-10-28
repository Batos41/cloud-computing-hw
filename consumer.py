import argparse
import json
import boto3
import logging
import time
from typing import Optional, Dict, Any

parser = argparse.ArgumentParser(description="A consumer program to interact with producers.")
parser.add_argument("-r", type=str, default="usu-cs5270-orangutan-requests", help="Source bucket for requests.")
parser.add_argument("-d", choices=["s3", "dynamo"], default="s3", help="Destination storage scheme.")
parser.add_argument("-b", type=str, default="usu-cs5270-orangutan-web", help="Storage bucket if using s3.")
parser.add_argument("-t", type=str, default="widgets", help="Dynamo table name if using dynamo.")
args = parser.parse_args()
request_bucket = args.r
storage_scheme = args.d
storage_bucket = args.b
dynamo_table = args.t

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

session = boto3.Session(region_name='us-east-1')
client = session.client('s3')
db = session.resource('dynamodb')
table = db.Table(dynamo_table)

def main():
    while True:
        key = get_next_key_from_bucket(request_bucket)
        if key:
            logger.info(f"Processing request file: {key}")
            file_content = get_file_from_s3(request_bucket, key)
            if storage_scheme == "s3":
                process_one_request_s3(file_content)
            elif storage_scheme == "dynamo":
                process_one_request_dynamo(file_content)
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

def handle_update_request_s3(parsed_request):
    #raise NotImplementedError
    pass

def handle_delete_request_s3(parsed_request):
    #raise NotImplementedError
    pass

def handle_create_request_s3(parsed_request):
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

def process_one_request_s3(json_string: str):
    parsed_request = json.loads(json_string)
    if parsed_request["type"] == "create":
        handle_create_request_s3(parsed_request)
    elif parsed_request["type"] == "delete":
        handle_delete_request_s3(parsed_request)
    elif parsed_request["type"] == "update":
        handle_update_request_s3(parsed_request)

def handle_create_request_dynamo(parsed_request):
    logger.info(f"Process create request for widget {parsed_request['widgetId']} in request {parsed_request['requestId']}")
    widget = make_widget_from_request_dynamo(parsed_request)
    owner = kebab_owner(parsed_request.get("owner"))
    key = f"widgets/{owner}/{widget['id']}"
    logger.info(f"Add to dynamo table {dynamo_table} a widget with key = {key}")
    table.put_item(Item=widget)

def handle_delete_request_dynamo(parsed_request):
    #raise NotImplementedError
    pass

def handle_update_request_dynamo(parsed_request):
    #raise NotImplementedError
    pass

def make_widget_from_request_dynamo(parsed_request: Dict[str, Any]) -> Dict[str, str]:
    widget = {}
    for key, value in parsed_request.items():
        if key == 'type' or key == 'requestId':
            # Skip these keys (remove them)
            continue
        elif key == 'widgetId':
            # Rename 'widgetId' to 'id'
            widget['id'] = value
        elif key == 'otherAttributes':
            # Expand 'otherAttributes' into individual fields
            for attr in value:
                widget[attr['name']] = attr['value']
        else:
            # Copy all other fields as is
            widget[key] = value
    return widget

def process_one_request_dynamo(json_string: str):
    parsed_request = json.loads(json_string)
    if parsed_request["type"] == "create":
        handle_create_request_dynamo(parsed_request)
    elif parsed_request["type"] == "delete":
        handle_delete_request_dynamo(parsed_request)
    elif parsed_request["type"] == "update":
        handle_update_request_dynamo(parsed_request)

if __name__ == "__main__":
    main()
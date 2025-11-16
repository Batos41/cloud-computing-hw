import argparse
import json
import boto3
import logging
import time
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

s3_client = None
db_resource = None

class BaseStorage(ABC):
    """
    Abstract Base Class for storage strategies.
    Defines the interface for processing widget requests.
    """
    
    @abstractmethod
    def process_request(self, json_string: str):
        """Processes a single request string."""
        pass

    def _kebab_owner(self, owner: str) -> str:
        """Helper to format owner string."""
        if not owner:
            return "unknown-owner"
        return owner.lower().replace(" ", "-")

    def _parse_and_route(self, json_string: str, handlers: dict):
        """Helper to parse JSON and route to the correct method."""
        # This will raise JSONDecodeError if content is invalid, caught in main()
        parsed_request = json.loads(json_string)  
        request_type = parsed_request.get("type")
        
        handler = handlers.get(request_type)
        if handler:
            handler(parsed_request)
        else:
            logger.error(f"Unknown request type '{request_type}' in request {parsed_request.get('requestId')}")

class S3Storage(BaseStorage):
    """Storage strategy for S3."""
    
    def __init__(self, bucket_name: str, client: Any):
        self.bucket_name = bucket_name
        self.client = client
        self.handlers = {
            "create": self._handle_create_request,
            "delete": self._handle_delete_request,
            "update": self._handle_update_request
        }
        logger.info(f"Initialized S3Storage for bucket: {self.bucket_name}")

    def process_request(self, json_string: str):
        self._parse_and_route(json_string, self.handlers)

    def _make_widget_from_request(self, parsed_request: Dict[str, Any]) -> Dict[str, Any]:
        """Creates the widget data structure for S3 storage."""
        widget = {}
        for key, value in parsed_request.items():
            if key in ('type', 'requestId'):
                continue
            elif key == 'widgetId':
                widget['id'] = value
            else:
                widget[key] = value
        return widget

    def _handle_create_request(self, parsed_request):
        logger.info(f"Process create request for widget {parsed_request['widgetId']} in request {parsed_request['requestId']}")
        widget = self._make_widget_from_request(parsed_request)
        widget_json = json.dumps(widget)
        owner = self._kebab_owner(parsed_request.get("owner"))
        key = f"widgets/{owner}/{widget['id']}" 
        
        logger.info(f"Add to S3 bucket {self.bucket_name} a widget with key = {key}")
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=widget_json,
            ContentType='application/json'
        )

    def _handle_delete_request(self, parsed_request):
        """Implements delete logic."""
        widget_id = parsed_request['widgetId']
        
        # NOTE: Assuming delete request has 'owner' to reconstruct the key.
        owner = self._kebab_owner(parsed_request.get("owner", ""))
        key = f"widgets/{owner}/{widget_id}"
        
        try:
            # Check if object exists
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            
            # Object exists, delete it
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted widget {widget_id} from S3 key {key}")

        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Per requirements, log a warning if not found
                logger.warning(
                    f"Widget {widget_id} (key: {key}) not found for deletion. No action taken."
                )
            else:
                logger.error(f"Error deleting widget {widget_id} from S3: {e}")
        except Exception as e:
            logger.error(f"Failed to delete widget {widget_id} from S3: {e}")

    def _handle_update_request(self, parsed_request):
        """Implements update logic (Suggestion 2)."""
        logger.warning(
            f"Received 'update' request for widget {parsed_request.get('widgetId')}, "
            "but updates are not yet implemented."
        )

class DynamoDBStorage(BaseStorage):
    """Storage strategy for DynamoDB."""
    
    def __init__(self, table_name: str, db_resource: Any):
        self.table_name = table_name
        # The table object is initialized here
        self.table = db_resource.Table(table_name)
        self.handlers = {
            "create": self._handle_create_request,
            "delete": self._handle_delete_request,
            "update": self._handle_update_request
        }
        logger.info(f"Initialized DynamoDBStorage for table: {self.table_name}")

    def process_request(self, json_string: str):
        self._parse_and_route(json_string, self.handlers)

    def _make_widget_from_request_dynamo(self, parsed_request: Dict[str, Any]) -> Dict[str, str]:
        """Creates the flattened widget item for DynamoDB storage."""
        widget = {}
        for key, value in parsed_request.items():
            if key in ('type', 'requestId'):
                continue
            elif key == 'widgetId':
                widget['id'] = value  
            elif key == 'otherAttributes':
                # Flatten 'otherAttributes'
                for attr in value:
                    if 'name' in attr and 'value' in attr:
                         widget[attr['name']] = attr['value']
            else:
                widget[key] = value
        
        # DynamoDB doesn't like empty strings, remove or handle them
        # Converting empty strings to None and then filtering out None values
        return {k: v for k, v in widget.items() if v is not None and v != ""}

    def _handle_create_request(self, parsed_request):
        logger.info(f"Process create request for widget {parsed_request['widgetId']} in request {parsed_request['requestId']}")
        widget_item = self._make_widget_from_request_dynamo(parsed_request)
        
        logger.info(f"Add to dynamo table {self.table_name} a widget with id = {widget_item.get('id')}")
        self.table.put_item(Item=widget_item)

    def _handle_delete_request(self, parsed_request):
        """Implements delete logic (Suggestion 2)."""
        widget_id = parsed_request['widgetId']
        key = {'id': widget_id}  

        try:
            # Check if item exists
            response = self.table.get_item(Key=key)
            
            if 'Item' not in response:
                logger.warning(
                    f"Widget {widget_id} not found in DynamoDB for deletion. No action taken."
                )
                return

            # Item exists, delete it
            self.table.delete_item(Key=key)
            logger.info(f"Deleted widget {widget_id} from DynamoDB table {self.table_name}")
            
        except Exception as e:
            logger.error(f"Failed to delete widget {widget_id} from DynamoDB: {e}")

    def _handle_update_request(self, parsed_request):
        """Implements update logic (Suggestion 2)."""
        logger.warning(
            f"Received 'update' request for widget {parsed_request.get('widgetId')}, "
            "but updates are not yet implemented."
        )

# ==================================================================
# S3 Polling Functions (Requires global s3_client)
# ==================================================================

def get_next_key_from_bucket(bucket_name: str) -> Optional[str]:
    # Use the global client initialized in main
    response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
    if response.get('Contents'):
        s3_key = response['Contents'][0]['Key']
        return s3_key
    else:
        return None

def get_file_from_s3(bucket_name: str, s3_key: str) -> str:
    # Use the global client initialized in main
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def delete_file_from_s3(bucket_name: str, s3_key: str):
    # Use the global client initialized in main
    s3_client.delete_object(Bucket=bucket_name, Key=s3_key)

# ==================================================================
# Main Polling Loop
# ==================================================================

def main():
    global s3_client, db_resource
    
    # ARGUMENT PARSING
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

    # BOTO3 SETUP
    session = boto3.Session(region_name='us-east-1')
    s3_client = session.client('s3')
    db_resource = session.resource('dynamodb')

    # Initialize the correct strategy
    storage_strategy: BaseStorage
    if storage_scheme == "s3":
        storage_strategy = S3Storage(storage_bucket, s3_client)
    elif storage_scheme == "dynamo":
        # Pass the global resource to the DynamoDBStorage
        storage_strategy = DynamoDBStorage(dynamo_table, db_resource)
    else:
        logger.critical(f"Invalid storage scheme '{storage_scheme}'. Exiting.")
        return

    logger.info(f"Consumer started. Polling bucket '{request_bucket}'. Strategy: '{storage_scheme}'.")

    # Main polling loop
    while True:
        key = None
        try:
            key = get_next_key_from_bucket(request_bucket)
            if key:
                logger.info(f"Found request file: {key}")
                file_content = get_file_from_s3(request_bucket, key)
                
                # Process using the chosen strategy
                storage_strategy.process_request(file_content)
                
                # Delete after successful processing
                delete_file_from_s3(request_bucket, key)
                logger.info(f"Successfully processed and deleted request file: {key}")
                
                # Continue immediately to check for more messages
                continue 
            
            else:
                # No request found, wait a bit (100ms)
                time.sleep(0.1)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON for key {key}. Deleting bad message. Error: {e}")
            if key:
                # Delete 'poison pill' message so we don't get stuck
                delete_file_from_s3(request_bucket, key)
        
        except ClientError as e:
             # Handle AWS service errors (e.g., permission denied)
             logger.error(f"AWS Client Error processing key {key}: {e}", exc_info=True)
             time.sleep(1) # Wait longer on AWS errors

        except Exception as e:
            # Catch-all for other processing errors
            logger.error(f"Unhandled error processing key {key}: {e}", exc_info=True)
            # Delete the key to prevent re-processing a persistent error
            if key:
                 delete_file_from_s3(request_bucket, key)
            time.sleep(1)


if __name__ == "__main__":
    main()
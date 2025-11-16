import json
import logging
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

import consumer
from consumer import DynamoDBStorage, S3Storage

# --- Sample Request Data (Test Fixtures) ---
WIDGET_ID = "8123f304-f23f-440b-a6d3-80e979fa4cd6"
OWNER = "Mary Matthews"
REQUEST_ID = "e80fab52-71a5-4a76-8c4d-11b66b83ca2a"

# Sample Create Request
CREATE_REQUEST = {
    "type": "create",
    "requestId": REQUEST_ID,
    "widgetId": WIDGET_ID,
    "owner": OWNER,
    "label": "JWJYY",
    "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
    "otherAttributes": [
        {"name": "width-unit", "value": "cm"},
        {"name": "rating", "value": "2.580677"}
    ]
}
CREATE_REQUEST_JSON = json.dumps(CREATE_REQUEST)

# Sample Delete Request
DELETE_REQUEST = {
    "type": "delete",
    "requestId": "a1b2c3d4-test-delete",
    "widgetId": WIDGET_ID,
    "owner": OWNER
}
DELETE_REQUEST_JSON = json.dumps(DELETE_REQUEST)

# Sample Update Request
UPDATE_REQUEST = {
    "type": "update",
    "requestId": "e1f2a3b4-test-update",
    "widgetId": WIDGET_ID,
    "owner": OWNER,
    "label": "New Label"
}
UPDATE_REQUEST_JSON = json.dumps(UPDATE_REQUEST)


class TestS3Storage(unittest.TestCase):

    def setUp(self):
        # Disable lower-level logging (keep WARNING+ enabled so assertLogs works)
        logging.disable(logging.INFO)
        
    # Create a mock for the Boto3 S3 client
        self.mock_s3_client = MagicMock()
        
    # Mock the ClientError exception
    # This is needed to simulate a "404 Not Found" error
        self.mock_s3_client.exceptions = MagicMock()
        self.mock_s3_client.exceptions.ClientError = ClientError
        
    # Instantiate our S3Storage class with the mock client
        self.storage = S3Storage(bucket_name="test-widget-bucket", client=self.mock_s3_client)

    def tearDown(self):
        # Re-enable logging
        logging.disable(logging.NOTSET)

    def test_kebab_owner(self):
        self.assertEqual(self.storage._kebab_owner("Mary Matthews"), "mary-matthews")
        self.assertEqual(self.storage._kebab_owner("John"), "john")
        self.assertEqual(self.storage._kebab_owner(None), "unknown-owner")

    def test_handle_create_request(self):
        self.storage.process_request(CREATE_REQUEST_JSON)

        # Verify the widget body that would be created
        expected_widget_body = {
            "id": WIDGET_ID,
            "owner": OWNER,
            "label": "JWJYY",
            "description": CREATE_REQUEST["description"],
            "otherAttributes": CREATE_REQUEST["otherAttributes"]
        }
        
        # Verify that put_object was called once with the correct arguments
        self.mock_s3_client.put_object.assert_called_once_with(
            Bucket="test-widget-bucket",
            Key="widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6",
            Body=json.dumps(expected_widget_body),
            ContentType='application/json'
        )

    def test_handle_delete_request_success(self):
        # No setup needed, the default MagicMock for head_object will just run
        self.storage.process_request(DELETE_REQUEST_JSON)

        # Verify we first checked if the object exists
        self.mock_s3_client.head_object.assert_called_once_with(
            Bucket="test-widget-bucket",
            Key="widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6"
        )
        
        # Verify we then deleted the object
        self.mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-widget-bucket",
            Key="widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6"
        )

    def test_handle_delete_request_not_found(self):
        # Configure the mock head_object to raise a 404 ClientError
        error_response = {'Error': {'Code': '404'}}
        self.mock_s3_client.head_object.side_effect = ClientError(error_response, 'HeadObject')

        # Use assertLogs to capture the WARNING
        with self.assertLogs('consumer', level='WARNING') as log_capture:
            self.storage.process_request(DELETE_REQUEST_JSON)

        # Verify the log message contains the required warning
        self.assertIn("not found for deletion. No action taken.", log_capture.output[0])
        
        # Verify delete_object was *not* called
        self.mock_s3_client.delete_object.assert_not_called()

    def test_handle_update_request(self):
        # Use assertLogs to capture the WARNING
        with self.assertLogs('consumer', level='WARNING') as log_capture:
            self.storage.process_request(UPDATE_REQUEST_JSON)

        # Verify the log message
        self.assertIn("but updates are not yet implemented.", log_capture.output[0])
        
        # Verify no S3 modifications were attempted
        self.mock_s3_client.put_object.assert_not_called()
        self.mock_s3_client.delete_object.assert_not_called()


class TestDynamoDBStorage(unittest.TestCase):

    def setUp(self):
        # Disable lower-level logging (keep WARNING+ enabled so assertLogs works)
        logging.disable(logging.INFO)
        
    # Create a mock for the Boto3 DynamoDB resource
        self.mock_db_resource = MagicMock()
        
    # Create a mock for the DynamoDB Table object
        self.mock_table = MagicMock()
        
    # Configure the resource's .Table() method to return our mock table
        self.mock_db_resource.Table.return_value = self.mock_table

        
    # Instantiate our DynamoDBStorage class with the mock resource
        self.storage = DynamoDBStorage(table_name="test-widget-table", db_resource=self.mock_db_resource)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_make_widget_from_request_dynamo(self):
        # Test the flattening logic
        widget_item = self.storage._make_widget_from_request_dynamo(CREATE_REQUEST)
        
        expected_item = {
            "id": WIDGET_ID,
            "owner": OWNER,
            "label": "JWJYY",
            "description": CREATE_REQUEST["description"],
            "width-unit": "cm",    # Flattened from otherAttributes
            "rating": "2.580677" # Flattened from otherAttributes
        }
        
        self.assertEqual(widget_item, expected_item)

    def test_handle_create_request(self):
        self.storage.process_request(CREATE_REQUEST_JSON)
        
        # Define the expected flattened item
        expected_item = {
            "id": WIDGET_ID,
            "owner": OWNER,
            "label": "JWJYY",
            "description": CREATE_REQUEST["description"],
            "width-unit": "cm",
            "rating": "2.580677"
        }

        # Verify that put_item was called once with the correct Item
        self.mock_table.put_item.assert_called_once_with(Item=expected_item)

    def test_handle_delete_request_success(self):
    # Configure get_item to return an item, simulating "found"
        self.mock_table.get_item.return_value = {'Item': {'id': WIDGET_ID}}

        self.storage.process_request(DELETE_REQUEST_JSON)
        
    # Verify we checked for the item with the correct key
        # (Note: key uses 'id' because your _make_widget... renames it)
        expected_key = {'id': WIDGET_ID}
        self.mock_table.get_item.assert_called_once_with(Key=expected_key)
        
    # Verify we then deleted the item with the correct key
        self.mock_table.delete_item.assert_called_once_with(Key=expected_key)

    def test_handle_delete_request_not_found(self):
        # Configure get_item to return an empty dict, simulating "not found"
        self.mock_table.get_item.return_value = {}

        # Use assertLogs to capture the WARNING
        with self.assertLogs('consumer', level='WARNING') as log_capture:
            self.storage.process_request(DELETE_REQUEST_JSON)

        # Verify the log message
        self.assertIn("not found in DynamoDB for deletion. No action taken.", log_capture.output[0])
        
        # Verify delete_item was *not* called
        self.mock_table.delete_item.assert_not_called()

    def test_handle_update_request(self):
        # Use assertLogs to capture the WARNING
        with self.assertLogs('consumer', level='WARNING') as log_capture:
            self.storage.process_request(UPDATE_REQUEST_JSON)

        # Verify the log message
        self.assertIn("but updates are not yet implemented.", log_capture.output[0])
        
        # Verify no DynamoDB modifications were attempted
        self.mock_table.put_item.assert_not_called()
        self.mock_table.delete_item.assert_not_called()


class TestPollingFunctions(unittest.TestCase):
    
    # We patch 'consumer.s3_client' which is the global client
    # used by the polling functions.
    
    @patch('consumer.s3_client')
    def test_get_next_key_from_bucket_success(self, mock_s3):
        # Configure the mock S3 client's response
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'request-001.json'}]
        }
        
        key = consumer.get_next_key_from_bucket("test-request-bucket")
        
        # Verify the key is returned
        self.assertEqual(key, "request-001.json")
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="test-request-bucket", MaxKeys=1
        )

    @patch('consumer.s3_client')
    def test_get_next_key_from_bucket_empty(self, mock_s3):
    # Configure response for an empty bucket
        mock_s3.list_objects_v2.return_value = {} # No 'Contents' key
        
        key = consumer.get_next_key_from_bucket("test-request-bucket")
        
    # Verify None is returned
        self.assertIsNone(key)

    @patch('consumer.s3_client')
    def test_get_file_from_s3(self, mock_s3):
    # Mock the file content stream
        mock_body = MagicMock()
        mock_body.read.return_value = b'{"foo": "bar"}' # S3 body is in bytes
        
    # Configure get_object to return the mock body
        mock_s3.get_object.return_value = {'Body': mock_body}
        
        content = consumer.get_file_from_s3("bucket", "key")
        
    # Verify the content is correct
        self.assertEqual(content, '{"foo": "bar"}')
        mock_s3.get_object.assert_called_once_with(Bucket="bucket", Key="key")

    @patch('consumer.s3_client')
    def test_delete_file_from_s3(self, mock_s3):
        consumer.delete_file_from_s3("bucket", "key")
        mock_s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key")


if __name__ == "__main__":
    unittest.main()
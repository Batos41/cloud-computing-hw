Repository for my homework for class 6270 - Cloud Computing

How to run the consumer (consumer.py)
------------------------------------

Prerequisites
- Python 3.8+ installed
- The `boto3` library available in the active Python environment
- AWS credentials configured in the environment (e.g., via the AWS CLI or environment variables)

Quick setup (PowerShell)

```powershell
# Create and activate a virtual environment (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install boto3
pip install boto3
```

Running the consumer

The consumer polls an S3 bucket of request files and writes processed widgets to either S3 or DynamoDB.

Examples (from the repository directory):

```powershell
# Run consumer with S3 as destination storage
python consumer.py -r <request-bucket-name> -d s3 -b <storage-bucket-name>

# Run consumer with DynamoDB as destination storage
python consumer.py -r <request-bucket-name> -d dynamo -t <dynamo-table-name>
```

Notes
- The consumer expects AWS credentials available to boto3 (environment variables, shared config, or instance role).
- The polling loop will automatically shut down if no requests are received for 30 seconds (idle timeout).
- For local unit testing, the test suite mocks AWS calls; you can run the tests with:

```powershell
python -m unittest test_consumer.py
```

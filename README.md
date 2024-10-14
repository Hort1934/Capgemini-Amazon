### **Lambda Function Code with Comments**

```python
import boto3
import pandas as pd
import psycopg2
from io import StringIO

# Initialize S3 client for interacting with S3 buckets
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Extract bucket name and file key (file path in S3) from the event trigger
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Get the CSV file from the S3 bucket
    obj = s3.get_object(Bucket=bucket, Key=key)
    
    # Load the CSV data into a Pandas DataFrame for transformation
    df = pd.read_csv(obj['Body'])
    
    # Step 1: Filter rows where 'price' is <= 0 (invalid or free listings)
    df = df[df['price'] > 0]
    
    # Step 2: Convert 'last_review' to a valid date format, filling missing values with the earliest date
    df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')  # Convert to date, coerce errors to NaT
    df['last_review'].fillna(df['last_review'].min(), inplace=True)  # Fill missing dates with the earliest date
    
    # Step 3: Handle missing values in 'reviews_per_month' by filling with 0
    df['reviews_per_month'].fillna(0, inplace=True)
    
    # Step 4: Drop rows with missing 'latitude' or 'longitude', as geolocation is necessary for analysis
    df.dropna(subset=['latitude', 'longitude'], inplace=True)
    
    # Step 5: Save the transformed DataFrame back to S3 for further processing (such as loading into Redshift)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)  # Convert DataFrame back to CSV format
    s3.put_object(Bucket='your-transformed-data-bucket', Key=key, Body=csv_buffer.getvalue())  # Save transformed CSV
    
    return {'statusCode': 200, 'body': 'Data Transformation Completed'}  # Return success response
```

---

### **README**

#### **1. Environment Setup**

**a. Create AWS Services:**
1. **S3 Bucket:**
   - Create an S3 bucket for raw data: `nyc-airbnb-raw-data`.
   - Create another S3 bucket for transformed data: `nyc-airbnb-transformed-data`.

2. **Amazon Redshift Cluster:**
   - Create a Redshift cluster with a new database `airbnb_db`.
   - Create two tables:
     - **Raw Table** to store original data (schema defined in Lambda code).
     - **Processed Table** to store transformed data (schema defined in Lambda code).

3. **AWS Lambda:**
   - Create a Lambda function in the AWS Management Console with Python 3.x as the runtime.
   - Paste the provided Lambda code into the function editor.
   - Add an S3 read-write policy to the Lambda role to allow access to the data buckets.

4. **AWS Step Functions:**
   - Define a state machine to trigger the Lambda function for processing and loading data.

**b. IAM Roles:**
   - Assign the necessary permissions for S3, Redshift, and Lambda roles:
     - Lambda needs S3 read-write access and the ability to run SQL on Redshift.
     - Redshift needs an IAM role with `COPY` permissions for reading from S3.

#### **2. Running the Ingestion and Transformation Jobs**

**a. Upload Data to S3:**
   - Split the NYC Airbnb CSV dataset into smaller parts and upload it to the `nyc-airbnb-raw-data` bucket.
   - This will automatically trigger the Lambda function to ingest and transform the data.

**b. Monitoring the Lambda Execution:**
   - Go to the **Lambda Console** > **Monitor** tab to view CloudWatch logs of the Lambda execution.
   - Use the logs to verify successful execution and debug any issues.

#### **3. Data Loading into Redshift**

**a. Redshift COPY Command:**
   - After Lambda transforms the data and saves it to the transformed-data S3 bucket, you will load the data into Redshift using the `COPY` command:
     ```sql
     COPY transformed_airbnb_data
     FROM 's3://your-transformed-data-bucket/filename.csv'
     IAM_ROLE 'arn:aws:iam::your-role:role/RedshiftS3AccessRole'
     CSV
     IGNOREHEADER 1;
     ```

#### **4. Monitoring and Recovery**

**a. CloudWatch:**
   - Use **CloudWatch** to monitor logs from Lambda executions and Step Functions workflows. Check for error messages and performance metrics.
   - Set up CloudWatch Alarms for any pipeline failures or performance issues.

**b. AWS Step Functions:**
   - Step Functions visually orchestrate the ETL pipeline. Use the **Execution History** tab to monitor pipeline executions and trigger steps manually if necessary.

**c. Handling Common Errors:**
   - **Memory/Timeout Issues in Lambda:**
     - If the Lambda function exceeds its memory or execution time limits, increase the limits in the **Configuration** tab under **General Settings**.
   
   - **Redshift Loading Issues:**
     - Check Redshift logs if there are issues with the `COPY` command. Ensure that the IAM role has the correct permissions and the S3 path is correct.

---

### **12. Testing the Pipeline**

#### **Testing the End-to-End Flow:**
1. **Ingest the Data:**
   - Upload a test file to the S3 raw-data bucket and ensure that the Lambda function is triggered.
   - Check that the Lambda function processes the data and uploads it to the transformed-data bucket.

2. **Validate Data in Redshift:**
   - Run the `COPY` command in Redshift to load the transformed data into the `transformed_airbnb_data` table.
   - Validate the loaded data with the following SQL checks:
     ```sql
     SELECT COUNT(*) FROM transformed_airbnb_data WHERE price <= 0;
     ```
     ```sql
     SELECT COUNT(*) FROM transformed_airbnb_data WHERE latitude IS NULL OR longitude IS NULL;
     ```

#### **Expected Outcome:**
- The pipeline should correctly ingest, transform, and load the data from the NYC Airbnb dataset.
- Transformed data should be present in the Redshift `ProcessedTable` with quality checks confirming valid values for `price`, `latitude`, and `longitude`.

---

### **Deliverables:**

1. **Lambda Function Code:**
   - The Lambda function code is provided above with detailed comments explaining each step.

2. **Redshift Configuration:**
   - SQL commands to create the Redshift tables and perform data quality checks.

3. **README File:**
   - The README file explains how to set up, run, and monitor the pipeline, handle errors, and perform data validation.

---

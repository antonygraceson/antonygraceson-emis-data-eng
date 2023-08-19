# Data Engineer - Antony Graceson Assessment for EMIS
In this assessment, I successfully leveraged the AWS Glue service in combination with the PySpark framework to design and implement a robust data processing and transformation pipeline, showcasing the power of serverless ETL and the scalability of PySpark for efficient data management.

## Output Attachment
In this repository, you will find an output Parquet file named "part-00000-33a924b3-1788-4a8a-8a97-9fadb0774947-c000.snappy.parquet." This file corresponds to the JSON data file 'Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json' and has been meticulously transformed into a structured, tabular format.

## Important Note
Running the script to process and convert all the data into this tabular format may take approximately one hour. This duration can vary based on the scale and complexity of the data under consideration. To ensure a smooth execution, please plan accordingly and allocate sufficient time for the script to complete its processing tasks.

## Steps 
## AWS Glue PySpark Script Execution Guide
This guide outlines the steps to set up and execute an AWS Glue PySpark script for transforming JSON data into a tabular format. Please follow these instructions carefully:

## Prerequisites
AWS Account: Ensure that you have access to an AWS account.

S3 Bucket and Data: Create an S3 bucket and upload your JSON data files to a folder within this bucket. Additionally, create another folder within the same bucket to serve as the output destination.

## Script Configuration

## AWS Glue PySpark Script Editor:

Access the AWS Glue service on your AWS console.
Create a new AWS Glue PySpark script editor.

## Job Configuration:

In the script editor, configure your job as follows:
Job Name: [Your Job Name]
IAM Role: [Attach the appropriate IAM role]
Glue Version: 4.0
Language: Python3
Worker Type: G1 X
Requested Number of Workers: 5

## Script Customization:

In your Glue script, locate the placeholders for s3_bucket, s3_folder_path, and output_path.
Replace these placeholders with the appropriate S3 bucket and folder paths that you've set up in your AWS environment.
Running the Script
Save your script with the updated configurations.

## Run the Glue script.

Processing Time: Please be aware that the script execution may take approximately one hour, depending on the size and complexity of the data in your S3 bucket. During this time, all JSON data in the specified S3 bucket folder will be processed and transformed into a tabular format.

## Conclusion
This setup allows you to efficiently convert JSON data into a structured, tabular format using AWS Glue and PySpark. Ensure that you have the necessary permissions and resources in your AWS environment to execute the script successfully.

For any issues or questions, please refer to AWS Glue documentation or seek assistance from AWS support.

Happy data transformation! 🚀
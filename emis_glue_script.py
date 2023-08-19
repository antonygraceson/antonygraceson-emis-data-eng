import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    def child_struct(nested_df):
        # Creating python list to store dataframe metadata
        list_schema = [((), nested_df)]
        # Creating empty python list for final flattern columns
        flat_columns = []

        while len(list_schema) > 0:
            # Removing latest or recently added item (dataframe schema) and returning into df variable
            parents, df = list_schema.pop()
            flat_cols = [col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if
                         c[1][:6] != "struct"]

            struct_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]

            flat_columns.extend(flat_cols)
            # Reading  nested columns and appending into stack list
            for i in struct_cols:
                projected_df = df.select(i + ".*")
                list_schema.append((parents + (i,), projected_df))
        return nested_df.select(flat_columns)


    def master_array(df):
        array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        while len(array_cols) > 0:
            for c in array_cols:
                df = df.withColumn(c, explode_outer(c))
            df = child_struct(df)
            array_cols = [c[0] for c in df.dtypes if c[1][:5] == "array"]
        return df


    # Define your S3 bucket and folder path
    s3_bucket = "test-dms-config"  # Replace s3 Bucket Path
    s3_folder_path = "data/"  # Replace Your Bucket Folder Path
    output_path = 'result'  # Replace Your Output Path

    # Initialize a Boto3 S3 client
    s3_client = boto3.client('s3')

    # List objects in the S3 folder
    s3_objects = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_folder_path)

    # Extract JSON file keys
    json_keys = [obj['Key'] for obj in s3_objects['Contents'] if obj['Key'].endswith(".json")]

    # Read JSON files from S3
    for json_key in json_keys:
        file_name = json_key.split("/")[-1].split(".json")[0]
        s3_uri = f"s3://{s3_bucket}/{json_key}"
        df_json = spark.read.option("multiLine", "true").json(s3_uri)
        df_json.printSchema()
        df_output = master_array(df_json)
        df_output.printSchema()
        dynamic_frame = DynamicFrame.fromDF(df_output, glueContext, "my_dynamic_frame")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": f"s3://{s3_bucket}/{output_path}/{file_name}",
                "compression": "snappy",  # Specify the desired compression type as "snappy"
                "parquet": "true"  # Specify that the output format should be Parquet
            },
            format="parquet"  # Specify the format as "parquet"
        )
    job.commit()

except Exception as e:
    # Handle the exception here, you can print an error message or take other actions.
    print("An error occurred:", str(e))
    # Optionally, you can raise the exception again to signal the failure.
    raise e

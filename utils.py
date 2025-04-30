# utils.py
import boto3
import pandas as pd
import yaml
import os
import datetime
import time


# Load Configuration
def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

config = load_config()

# AWS Session Setup
def assume_role(role_arn):
    sts_client = boto3.client("sts")
    assumed_role = sts_client.assume_role(RoleArn=role_arn, RoleSessionName="CrossAccountSession")
    credentials = assumed_role["Credentials"]
    return boto3.session.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )

# Read SQL Query from File
def read_sql_query(query_file):
    with open(os.path.join("Query", query_file), "r") as file:
        return file.read()

# Athena Query Execution
def query_athena(query, database, output_location, session=None):
    client = (session or boto3).client("athena")
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    return response["QueryExecutionId"]

# Wait for Athena Query to Complete
def wait_for_query_to_complete(client, execution_id, poll_interval=5):
    print("Waiting for Athena query to complete...")
    
    while True:
        status = client.get_query_execution(QueryExecutionId=execution_id)
        query_status = status["QueryExecution"]["Status"]
        state = query_status["State"]
        
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print(f"Query completed with status: {state}")
            
            if state in ["FAILED", "CANCELLED"]:
                reason = query_status.get("StateChangeReason", "No reason provided.")
                print(f"Query failed or cancelled. Reason: {reason}")            
            return state        
        time.sleep(poll_interval)

def to_dataframe(s3_client, execution_output_path):
    # Parse bucket and key from full s3 path
    path = execution_output_path.replace("s3://", "")
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])

    print(f"Reading result file from S3: s3://{bucket}/{key}")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(f"Retrieved {len(df)} records from S3.")
    return df

# Read Athena Tables from Multiple Queries
def read_athena_tables(query_files, database, output_location, session=None):
    all_dataframes = []
    for query_file in query_files:
        query = read_sql_query(query_file)
        execution_id = query_athena(query, database, output_location, session)
        client = (session or boto3).client("athena")

        state = wait_for_query_to_complete(client, execution_id)
        if state != "SUCCEEDED":
            raise Exception(f"Athena query failed with status: {state}")

        #This will get only 1000 records
        #result = client.get_query_results(QueryExecutionId=execution_id)
        #df = pd.DataFrame([row["Data"] for row in result["ResultSet"]["Rows"][1:]])

        #This will get all records
        execution = client.get_query_execution(QueryExecutionId=execution_id)
        result_path = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

        s3_client = session.client("s3")
        df = to_dataframe(s3_client, result_path)

        all_dataframes.append(df)
    return all_dataframes
    

# Write Data to S3
def write_to_s3(dataframe, bucket, key, file_format="csv", session=None):
    dataframe["loaddts"] = datetime.datetime.utcnow()
    s3_client = (session or boto3).client("s3")
    
    if file_format == "csv":
        buffer = dataframe.to_csv(index=False)
    elif file_format == "parquet":
        import io
        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)
    elif file_format == "json":
        buffer = dataframe.to_json(orient="records")
    else:
        raise ValueError("Unsupported file format")
    
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer)

# Read from S3
def read_from_s3(bucket, key, file_format="csv", session=None):
    s3_client = (session or boto3).client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    
    if file_format == "csv":
        return pd.read_csv(response["Body"])
    elif file_format == "parquet":
        import io
        return pd.read_parquet(io.BytesIO(response["Body"].read()))
    elif file_format == "json":
        return pd.read_json(response["Body"], orient="records")
    else:
        raise ValueError("Unsupported file format")

# process_data.py
if __name__ == "__main__":
    session = assume_role(config["aws_role"])
    dataframes = read_athena_tables(config["query_files"], config["source_database"], config["athena_output_location"], session)
    
    for i, df in enumerate(dataframes):
        df["new_column"] = df["existing_column"] * 2  # Example transformation
        write_to_s3(df, config["destination_bucket"], f"processed_data_{i}", file_format=config["file_format"], session=session)
    
    print("Data processing complete!")

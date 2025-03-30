# AWS Athena Data Pipeline

## Overview
This project is designed to read data from AWS Athena tables, perform transformations, and write the processed data back to AWS S3 in various formats (CSV, Parquet, JSON). It also supports cross-account access using IAM roles and allows SQL queries to be managed separately in a `Query` folder. The pipeline follows best practices with structured logging and modular design.

## Features
- Reads queries from `.sql` files and executes them in AWS Athena.
- Retrieves query results and loads them into Pandas dataframes.
- Performs transformations on the retrieved data.
- Writes the processed data back to AWS S3 in user-specified formats.
- Supports cross-account access via IAM roles.
- Uses structured logging for better debugging and monitoring.

## Prerequisites
Before running the project, ensure the following:
1. **AWS Credentials**: The system should have AWS credentials configured (via `aws configure` or IAM role).
2. **Python Version**: Python 3.x is required.
3. **Required Python Libraries**: Install dependencies using:
   ```sh
   pip install -r requirements.txt
   ```

## Configuration
Update the `config.yaml` file with the necessary parameters:
```yaml
aws_role: "arn:aws:iam::123456789012:role/CrossAccountRole"
source_database: "source_db"
destination_database: "destination_db"
query_files:
  - "./query/query1.sql"
  - "./query/query2.sql"
  - "./query/query3.sql"
athena_output_location: "s3://your-athena-query-results-bucket/"
destination_bucket: "your-destination-bucket"
file_format: "csv"  # or "parquet" or "json"
```

## Running the Pipeline
To execute the pipeline, run:
```sh
python process_data.py
```

## Project Structure
```
aws-athena-data-pipeline/
│── Query/
│   ├── query1.sql
│   ├── query2.sql
│   ├── query3.sql
│── utils.py
│── config.yaml
│── process_data.py
│── requirements.txt
│── README.md
```

## Code Breakdown
### 1. `utils.py`
- Loads the configuration from `config.yaml`.
- Assumes an IAM role for cross-account access.
- Reads SQL queries from `.sql` files.
- Executes queries in AWS Athena and retrieves results.
- Reads/writes data from/to AWS S3 in multiple formats.

### 2. `process_data.py`
- Loads the configuration and sets up the AWS session.
- Reads Athena tables using the provided SQL queries.
- Transforms the data by adding new columns or modifying existing ones.
- Writes the processed data back to AWS S3.
- Implements structured logging directly within the script.

## Technologies Used
- **AWS Athena**: Executes SQL queries on S3 data.
- **AWS S3**: Stores query results and processed data.
- **IAM Roles**: Enables secure cross-account data access.
- **Python Libraries**:
  - `boto3`: AWS SDK for interacting with Athena and S3.
  - `pandas`: Data processing and transformation.
  - `pyyaml`: Parses YAML configuration files.
  - `logging`: Implements structured logging.

## Enhancements & Future Scope
- Implement parallel execution for multiple queries.
- Add support for automated monitoring and alerts.
- Extend transformations using more complex data processing logic.

## License
This project is open-source and free to use. Modify as per your requirements!


import utils
from logger import app_logger

if __name__ == "__main__":
    app_logger.info("Starting data processing script")
    config = utils.load_config()
    session = utils.assume_role(config["aws_role"])
    
    app_logger.info("Fetching data from Athena tables")
    dataframes = utils.read_athena_tables(
        config["query_files"], 
        config["source_database"], 
        config["athena_output_location"], 
        session
    )
    
    for i, df in enumerate(dataframes):
        if not df.empty:
            app_logger.info(f"Processing dataframe {i}")
            df["new_column"] = df["existing_column"] * 2  # Example transformation
            utils.write_to_s3(
                df, 
                config["destination_bucket"], 
                f"processed_data_{i}.{config["file_format"]}", 
                file_format=config["file_format"], 
                session=session
            )
            app_logger.info(f"Successfully wrote processed_data_{i}.{config["file_format"]} to S3")
    
    app_logger.info("Data processing complete!")

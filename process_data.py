import utils

if __name__ == "__main__":
    config = utils.load_config()
    session = utils.assume_role(config["aws_role"])
    
    dataframes = utils.read_athena_tables(
        config["query_files"], 
        config["source_database"], 
        config["athena_output_location"], 
        session
    )
    
    for i, df in enumerate(dataframes):
        if not df.empty:
            df["new_column"] = df["existing_column"] * 2  # Example transformation
            utils.write_to_s3(
                df, 
                config["destination_bucket"], 
                f"processed_data_{i}.{config["file_format"]}", 
                file_format=config["file_format"], 
                session=session
            )
    
    print("Data processing complete!")

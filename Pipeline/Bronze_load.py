import logging

# Get the Databricks logger
logger = logging.getLogger("py4j")

# won't work on serverless compute
#log4jLogger = spark._jvm.org.apache.log4j
#logger = log4jLogger.LogManager.getLogger("DATABASE_PIPELINE")

#instead
logger.setLevel(logging.INFO)

def merge_to_bronze(microbatch, batchID):

    # excluding _change_type, _commit_version, etc as source is changeDataFeed enabled
    data_columns = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", 
                    "c_acctbal", "c_mktsegment", "c_comment", "status", 
                    "updated_on", "updated_by", "created_on", "created_by"]

    cleaned_batch = microbatch.filter("_change_type IN ('insert', 'update_postimage')").select(*data_columns)

    batch_count = microbatch.count()
    batch_count = cleaned_batch.count()
    if batch_count > 0:
        logger.info(f"APP_LOG |--- Processing Batch ID: {batchID} | Size: {batch_count} rows ---")
        #print(f"--- Processing Batch ID: {batchID} | Size: {batch_count} rows ---")
        cleaned_batch.createOrReplaceTempView("temp_view_bronze")
        spark.sql("""
                  INSERT INTO workspace.etl_pipe.customer_bronze 
                  SELECT * FROM temp_view_bronze S
                  LEFT ANTI JOIN workspace.etl_pipe.customer_bronze T
                  ON S.c_custkey = T.c_custkey AND S.updated_on = T.updated_on
                  """)
    else:
        logger.info(f"APP_LOG |--- Batch {batchID} had no relevant changes. Skipping. ---")
        #print(f"--- Batch {batchID} had no relevant changes. Skipping. ---")

def run_pipeline():

    bronze_stream = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("readChangeFeed", "true")   
        .option("startingVersion", "0")    # Start from the very beginning of the source
        .option("maxFilesPerTrigger", 500) # Prevents grabbing all 100M at once
        .table("workspace.etl_pipe.customer_raw")
        .writeStream
        .foreachBatch(merge_to_bronze)
        .option("checkpointLocation","/Workspace/Users/nocode.yash@gmail.com/ETL_PIPE/checkpoints/bronze")
        .trigger(availableNow=True)
        .start()
        )
    logger.info("APP_LOG |Stream starting...")
    #print("Stream starting...")
    bronze_stream.awaitTermination() 
    logger.info("APP_LOG |Bronze stream fully completed.")
    #print("Bronze stream fully completed.")
    
if __name__ == "__main__":
    logger.info("APP_LOG |Starting pipe for bronze...")
    #print("Starting pipe for bronze...")
    run_pipeline()
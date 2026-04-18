from pyspark.sql.functions import current_timestamp, row_number, col
from pyspark.sql import Window
import logging

# Get the Databricks logger
logger = logging.getLogger("py4j")

def merge_to_silver(microbatch,batchId):
    #window spec for primary key
    window_spec = Window.partitionBy("c_custkey").orderBy(col("_commit_version").desc())

    #latest updates
    latest_updates = (
        microbatch
        .withColumn("rank", row_number().over(window_spec))
        .filter("rank=1")
        .select("c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment", "status", "updated_on", "updated_by", "created_on", "created_by")
        .withColumn("silver_updated", current_timestamp())
    )
    latest_updates.createOrReplaceTempView("latest_updates")
    count_value = spark.sql("SELECT count(*) FROM latest_updates").collect()[0][0]
    logger.info(f"APP_LOG |Batch {batchId}: Processing {count_value} rows.")
    #print(f"Batch {batchId}: Processing {count_value} rows.")

    # Merge logic SCD1
    spark.sql(
        """
        MERGE INTO workspace.etl_pipe.customer_silver AS A
        USING latest_updates AS B
        ON A.c_custkey=B.c_custkey
        WHEN MATCHED AND (
                        A.updated_on IS DISTINCT FROM B.updated_on 
                        AND B.updated_on > A.updated_on
                    )
        THEN
        UPDATE SET A.c_name=B.c_name,
                    A.c_address=B.c_address,
                    A.c_nationkey=B.c_nationkey,
                    A.c_phone=B.c_phone,
                    A.c_acctbal=B.c_acctbal,
                    A.c_mktsegment=B.c_mktsegment,
                    A.c_comment=B.c_comment,
                    A.status=B.status,
                    A.updated_on=B.updated_on,
                    A.updated_by=B.updated_by,
                    A.silver_updated = B.silver_updated
        WHEN NOT MATCHED THEN
        INSERT *
        """
    )
    # not updating created_by/created_on as that would be constant


def run_pipeline():
    silver_query = (
        spark.readStream
        .option("readChangeFeed","true")
        .table("workspace.etl_pipe.customer_bronze")
        .writeStream
        .trigger(availableNow=True)
        .foreachBatch(merge_to_silver)
        .option("checkpointLocation","/Workspace/Users/nocode.yash@gmail.com/ETL_PIPE/checkpoints/silver")
        .start()
    )
    # Keep the job running
    silver_query.awaitTermination()
    logger.info("APP_LOG |Silver stream completed....")
    #print("Silver stream completed....")

# 4. Entry Point
if __name__ == "__main__":
    run_pipeline()

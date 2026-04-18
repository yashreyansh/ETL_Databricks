

def load_to_silver(miniBatch, batchID):
    miniBatch.createOrReplaceTempView("miniBatch")
    spark.sql("""
              MERGE INTO  workspace.etl_pipe.order_silver A USING
              miniBatch B
              ON 
                A.o_orderkey = B.o_orderkey AND 
                (
                    (B.updated_on is NOT null and B.Latest_record is null) or B.Latest_record='Y'
                    -- find only the latest records
                ) and A.updated_on <> B.updated_on
              WHEN MATCHED 
                            THEN UPDATE SET *
              WHEN NOT MATCHED 
                     AND --A.o_orderkey <> B.o_orderkey AND  -- for new records
              (
                    (B.updated_on is NOT null and B.Latest_record is null) or B.Latest_record='Y'
                    -- find only the latest records
                ) 
                            THEN INSERT *
                            
              """)
    
def pipeline():
    print("Starting the data load...")
    silver_checkpoint = "/Workspace/Users/nocode.yash@gmail.com/ETL_PIPE/checkpoints/order_silver"
    silver_query =(
        spark.readStream\
            #.option("readChangeFeed","true")
            .table("workspace.etl_pipe.order_bronze")\
                .writeStream\
                    .option("skipChangeCommits", "true")
            .option("startingVersion","0")
            .option("maxFilesPerTrigger", 500)
                    .trigger(availableNow=True)\
                    .foreachBatch(load_to_silver)\
                    .option("checkpointLocation", silver_checkpoint)\
                    .start()
    )
    silver_query.awaitTermination()
    print("Data load completed.")


if __name__ == "__main__":
    pipeline()


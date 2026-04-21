def load_order_bronze(microbatch, batchID):
    #Logic to INSERT updated records, SCD2
    # on silver we can window per updated_time or latest record
    cleaned_batch = microbatch.filter("_change_type IN ('insert', 'update_postimage')")
    cleaned_batch = cleaned_batch.drop("_change_type", "_commit_version", "_commit_timestamp")

    batch_count = microbatch.count()
    batch_count = cleaned_batch.count()
    if batch_count > 0:
        cleaned_batch.createOrReplaceTempView("miniBatch")
        '''
        spark.sql("""
                INSERT INTO workspace.etl_pipe.order_bronze  
                SELECT A.* 
                 from miniBatch A
                LEFT ANTI  JOIN  workspace.etl_pipe.order_bronze B
                    ON A.o_orderkey=B.o_orderkey AND A.updated_on=B.updated_on  
                """)
        '''
        # logic to remove window function in future in silver load
        spark.sql("""
                INSERT INTO workspace.etl_pipe.order_bronze  
                SELECT A.*  , 'Y' as Latest_record --for latest row flag
                 from miniBatch A
                LEFT ANTI  JOIN  workspace.etl_pipe.order_bronze B
                    ON A.o_orderkey=B.o_orderkey AND A.updated_on=B.updated_on  
                """)
        spark.sql("""
                  MERGE INTO workspace.etl_pipe.order_bronze  A
                  USING miniBatch B
                  ON  A.o_orderkey=B.o_orderkey AND A.updated_on<B.updated_on
                  WHEN MATCHED THEN UPDATE SET A.Latest_record='N'
                  """)
        
    else:
        print("Batch is empty..")

def pipeline():
    bronze_stream = (
        spark.readStream
        .option("readChangeFeed","true")
        .option("skipChangeCommits", "true")
        .option("startingVersion","0")
        .option("maxFilesPerTrigger", 500)
        .table("workspace.etl_pipe.orders_raw")
        .writeStream
        .foreachBatch(load_order_bronze)
        .option("checkpointLocation","/Workspace/Users/nocode.yash@gmail.com/ETL_PIPE/checkpoints/bronze_order")
        .trigger(availableNow=True)
        .start()
    )
    bronze_stream.awaitTermination()

if __name__ == "__main__":
    pipeline()
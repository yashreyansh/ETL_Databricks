


def gold_stream():

    customer_df = spark.readStream.format("delta").table("workspace.etl_pipe.customer_silver")
    order_df = spark.readStream.format("delta").table("workspace.etl_pipe.order_silver")

    join_df = order_df.join(customer_df



if __name__ =="__main__":

    gold_stream()
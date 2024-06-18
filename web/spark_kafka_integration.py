import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, sum, count, expr, approx_count_distinct, when, countDistinct,col,count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp,current_timestamp,rank

def process_kafka_stream():

    findspark.init()
    spark = SparkSession.builder.appName("PySparkKafkaIntegration") \
                               .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
                               .config("spark.sql.catalog.client", "com.datastax.spark.connector.datasource.CassandraCatalog") \
                               .config("spark.sql.catalog.client.spark.cassandra.connection.host", "127.0.0.1") \
                               .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
                               .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                               .getOrCreate()
 



    kafka_broker = "localhost:9092"
    kafka_topic = "ecom"

    kafkaStream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .load()
    )


    valueSchema = kafkaStream.selectExpr("CAST(value AS STRING)")

    schema = StructType([
    StructField("Sales ID", StringType()),
    StructField("Order ID", StringType()),
    StructField("Age", IntegerType()),
    StructField("Order Total", DoubleType()),
    StructField("Promo Code or Discount", StringType()),
    StructField("Product ID", IntegerType()),
    StructField("Product Name", StringType()),
    StructField("Quantity Sold", IntegerType()),
    StructField("Unit Price", DoubleType()),
    StructField("Total Sales Amount", DoubleType()),
    StructField("Discount Amount", DoubleType()),
    StructField("Tax Amount", DoubleType()),
    StructField("Shipping Fee", DoubleType()),
    StructField("Transaction Date", LongType()),
    StructField("Transaction Date Readable", StringType()),
    StructField("Customer ID", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Payment Method", StringType()),
    StructField("Payment Status", StringType()),
    StructField("Transaction Status", StringType()),
    StructField("Transaction Type", StringType()),
    StructField("Location/Store", StringType()),
    StructField("Sales Representative", StringType()),
    StructField("Refund/Return Status", StringType()),
    StructField("Payment Confirmation Number", StringType()),
    StructField("Session ID", IntegerType()),  # New field for Session ID
    StructField("Feature Accessed", StringType()),  # New field for Feature Accessed
    StructField("Session Duration", StringType())
])

    parsedData = valueSchema.select(from_json("value", schema).alias("data")).select("data.*")

    def predict_sales_revenue(train_data):
        assembler = VectorAssembler(
            inputCols=["Transaction Date"],
            outputCol="features"
        )

        rf = RandomForestRegressor(
            labelCol="Total Sales Amount",
            featuresCol="features",
            numTrees=10
        )

        pipeline = Pipeline(stages=[assembler, rf])

        model = pipeline.fit(train_data)

        return model

    def process_batch(batch_df, batch_id):

        if batch_df.count() == 0:
            print("No data in the current batch. Skipping processing.")
            return
    
        revenue_total_sales_df = batch_df.groupBy().agg(
            sum("Total Sales Amount").alias("total_sales"),
            (sum("Total Sales Amount") - sum("Discount Amount")).alias("revenue"),
            (sum("Total Sales Amount") / count("Order ID")).alias("avg_sales"),
            sum("Quantity Sold").alias("total_inventory_value")
        )

        # Adding new columns
        revenue_total_sales_df = revenue_total_sales_df.withColumn("batch_id", expr("uuid()"))
        revenue_total_sales_df = revenue_total_sales_df.withColumn("stored_at", current_timestamp())
        revenue_total_sales_df.show()

        # Check if the table is empty
        existing_rows_count = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "revenue_total_sales") \
            .load() \
            .count()

# Writing to Cassandra with append or overwrite mode based on row count
        if existing_rows_count == 0:
            revenue_total_sales_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "revenue_total_sales") \
            .save()
        else:
            revenue_total_sales_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite")\
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "revenue_total_sales") \
            .option("confirm.truncate", "true") \
            .save()
        
        clv_df = batch_df.groupBy("Customer ID").agg(sum("Total Sales Amount").alias("customer_lifetime_value"))
        clv_df=clv_df.withColumnRenamed("Customer ID","customer_id")
        clv_df=clv_df.withColumn("stored_at",current_timestamp())
        clv_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "customer_lifetime_value") \
            .save()

        
    
        website_traffic_df = batch_df.select(approx_count_distinct("Customer ID").alias("WebsiteTraffic"))
        website_traffic_df = website_traffic_df.withColumnRenamed("WebsiteTraffic", "website_traffic")
        website_traffic_df = website_traffic_df.withColumn("batch_id", expr("uuid()"))
        website_traffic_df = website_traffic_df.withColumn("stored_at", current_timestamp())
        website_traffic_df.show()

        website_traffic_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "website_traffic") \
            .save()
        
        funnel_metrics_df = batch_df.groupBy("Transaction Status").agg(count("Order ID").alias("orders_count"))
        total_transactions = batch_df.count()
        bounce_rate_df = funnel_metrics_df.withColumn("batch_id", expr("uuid()"))
        bounce_rate_df = bounce_rate_df.withColumnRenamed("Transaction Status", "transaction_status")
        bounce_rate_df = bounce_rate_df.withColumn("bounce_rate", (col("orders_count") / total_transactions) * 100)
        bounce_rate_df = bounce_rate_df.withColumn("stored_at", current_timestamp())
        bounce_rate_df.show()

        bounce_rate_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "bounce_rate") \
            .save()

        stock_levels_df = batch_df.groupBy("Product Name").agg(sum("Quantity Sold").alias("StockSold"))
        stock_levels_df = stock_levels_df.withColumn("batch_id", expr("uuid()"))
        stock_levels_df = stock_levels_df.withColumnRenamed("Product Name","product_name")
        stock_levels_df = stock_levels_df.withColumn("stored_at", current_timestamp())
        stock_levels_df = stock_levels_df.withColumnRenamed("StockSold","stock_sold")
        stock_levels_df.show()

        stock_levels_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "stock_levels") \
            .save()


        inventory_turnover_df = batch_df.groupBy("Product Name").agg(
           (sum("Quantity Sold") / countDistinct("Transaction Date Readable")).alias("inventory_turnover")
        )
        inventory_turnover_df=inventory_turnover_df.withColumnRenamed("Product Name","product_name")
        inventory_turnover_df=inventory_turnover_df.withColumn("stored_at", current_timestamp())
        inventory_turnover_df.show()
        inventory_turnover_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "inventory_turnover") \
            .option("confirm.truncate", "true") \
            .save()

        gender_counts_df = batch_df.groupBy("Gender").agg(count("Customer ID").alias("customer_count"))
        gender_counts_df=gender_counts_df.withColumnRenamed("Gender","gender")
        gender_counts_df = gender_counts_df.withColumn("stored_at", current_timestamp())
        gender_counts_df.show()
        gender_counts_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "gender_counts") \
            .save()

        age_group_df = batch_df.withColumn(
            "AgeGroup",
            when((col("Age") >= 18) & (col("Age") <= 35), "Young Age")
            .when((col("Age") >= 36) & (col("Age") <= 60), "Middle Age")
            .when(col("Age") > 60, "Old Age")
            .otherwise("Unknown")
        ).groupBy("AgeGroup").agg(count("Customer ID").alias("customer_count"))
        age_group_df=age_group_df.withColumnRenamed("AgeGroup","age_group")
        age_group_df = age_group_df.withColumn("stored_at", current_timestamp())
        age_group_df.show()
        age_group_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "age_group") \
            .save()

        payment_method_counts_df = batch_df.groupBy("Payment Method").agg(count("Customer ID").alias("customer_count"))
        payment_method_counts_df=payment_method_counts_df.withColumnRenamed("Payment Method","payment_method")
        payment_method_counts_df = payment_method_counts_df.withColumn("stored_at", current_timestamp())
        payment_method_counts_df.show()
        payment_method_counts_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "payment_method_counts") \
            .save()

        top_selling_products_df = batch_df.groupBy("Product Name").agg(sum("Quantity Sold").alias("total_quantity_sold"))
        top_selling_products_df = top_selling_products_df.orderBy(col("total_quantity_sold").desc())
        top_selling_products_df=top_selling_products_df.withColumnRenamed("Product Name","product_name")
        top_selling_products_df = top_selling_products_df.withColumn("stored_at", current_timestamp())
        top_selling_products_df.show()
        top_selling_products_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "top_selling_products") \
            .save()

         # Analyze product performance over time
        product_performance_df = batch_df.groupBy("Product Name", "Transaction Date Readable").agg(
            sum("Quantity Sold").alias("quantity_sold"),
            sum("Total Sales Amount").alias("total_sales_amount")
        )
        product_performance_df=product_performance_df.withColumnRenamed("Product Name","product_name")
        product_performance_df=product_performance_df.withColumnRenamed("Transaction Date Readable","transaction_date_readable")
        product_performance_df = product_performance_df.withColumn("stored_at", current_timestamp())

        product_performance_df.show()
        product_performance_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "product_performance") \
            .option("confirm.truncate", "true") \
            .save()

        product_profitability_df = batch_df.groupBy("Product Name").agg(
            sum("Total Sales Amount").alias("total_sales_amount"),
            sum("Discount Amount").alias("total_discount_amount"),
            sum("Tax Amount").alias("total_tax_amount"),
            sum("Shipping Fee").alias("total_shipping_fee"),
            sum("Quantity Sold").alias("total_quantity_sold")
        )

        product_profitability_df = product_profitability_df.withColumn(
            "Profit",
            col("total_sales_amount") - col("total_discount_amount") - col("total_tax_amount") - col("total_shipping_fee")
        )

        product_profitability_df = product_profitability_df.orderBy(col("Profit").desc())
        product_profitability_df=product_profitability_df.withColumnRenamed("Profit","profit")
        product_profitability_df=product_profitability_df.withColumnRenamed("Product Name","product_name")
        product_profitability_df = product_profitability_df.withColumn("stored_at", current_timestamp())
        product_profitability_df.show()
        product_profitability_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "product_profitability") \
            .save()


        sales_rep_performance_df = batch_df.groupBy("Sales Representative").agg(
            sum("Total Sales Amount").alias("total_sales_amount"),
            sum("Quantity Sold").alias("total_quantity_sold")
        )

        sales_rep_performance_df = sales_rep_performance_df.orderBy(col("total_sales_amount").desc())
        sales_rep_performance_df=sales_rep_performance_df.withColumnRenamed("Sales Representative","sales_representative")
        sales_rep_performance_df=sales_rep_performance_df.withColumn("stored_at",current_timestamp())
        sales_rep_performance_df.show()
        product_profitability_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "product_profitability") \
            .save()
        
        location_performance_df = batch_df.groupBy("Location/Store").agg(
            sum("Total Sales Amount").alias("total_sales_amount"),
            sum("Quantity Sold").alias("total_quantity_sold")
        )
        location_performance_df=location_performance_df.withColumn("stored_at",current_timestamp())
        location_performance_df=location_performance_df.withColumnRenamed("Location/Store","location_store")
        location_performance_df = location_performance_df.orderBy(col("total_sales_amount").desc())
        location_performance_df.show()
        location_performance_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "location_performance") \
            .option("confirm.truncate", "true") \
            .save()

        avg_order_value_df = batch_df.groupBy("Location/Store").agg(
              (sum("Total Sales Amount") / countDistinct("Order ID")).alias("average_order_value")
        )
        avg_order_value_df=avg_order_value_df.withColumnRenamed("Location/store","location_store")
        avg_order_value_df=avg_order_value_df.withColumn("stored_at",current_timestamp())

        avg_order_value_df = avg_order_value_df.orderBy(col("average_order_value").desc())
        avg_order_value_df.show()
        avg_order_value_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "avg_order_value") \
            .save()

        rfm_df = batch_df.groupBy("Customer ID").agg(
            expr("max(`Transaction Date`) as LastPurchaseDate"),
            countDistinct("Order ID").alias("frequency"),
            sum("Total Sales Amount").alias("monetary")
        )

        current_timestamp_unix = unix_timestamp(current_timestamp())
        rfm_df = rfm_df.withColumn(
            "Recency",
            (current_timestamp_unix - col("LastPurchaseDate")) / (60 * 60 * 24)
        )
        rfm_df=rfm_df.withColumnRenamed("Customer ID","customer_id")
        rfm_df=rfm_df.withColumnRenamed("Recency","recency")
        rfm_df=rfm_df.withColumn("stored_at",current_timestamp())   

        rfm_df = rfm_df.drop("LastPurchaseDate")

        rfm_df.show()
        rfm_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("overwrite") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "rfm") \
            .option("confirm.truncate", "true") \
            .save()

        app_usage_analysis_df = batch_df.groupBy("Customer ID").agg(
                countDistinct("Session ID").alias("total_sessions"),
                sum("Session Duration").alias("total_session_duration"),
                count("Feature Accessed").alias("total_features_accessed")
        )

        app_usage_analysis_df = app_usage_analysis_df.withColumnRenamed("Customer ID", "customer_id")
        app_usage_analysis_df = app_usage_analysis_df.withColumn("stored_at", current_timestamp())

        app_usage_analysis_df.show()

        app_usage_analysis_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .option("spark.cassandra.connection.host", "127.0.0.1") \
                .option("spark.cassandra.connection.port", "9042") \
                .option("keyspace", "ecom_data") \
                .option("table", "app_usage_analysis") \
                .save()

        avg_session_duration_df = app_usage_analysis_df.select(
                "customer_id",
                F.current_timestamp().alias("stored_at"),  # Use current timestamp as stored_at
                (F.col("total_session_duration") / F.col("total_sessions")).alias("avg_session_duration")
        )

# Show the resulting DataFrame
        avg_session_duration_df.show()

# Write to Cassandra
        avg_session_duration_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("spark.cassandra.connection.host", "127.0.0.1") \
            .option("spark.cassandra.connection.port", "9042") \
            .option("keyspace", "ecom_data") \
            .option("table", "avg_session_duration") \
            .save()
        assembler = VectorAssembler(
            inputCols=["Transaction Date"],
            outputCol="features"
        )

        model = predict_sales_revenue(batch_df)

        predictions = model.transform(batch_df)

        evaluator = RegressionEvaluator(
            labelCol="Total Sales Amount",
            predictionCol="prediction",
            metricName="mse"
        )
        predictions.select("Transaction Date Readable", "Total Sales Amount", "prediction").show()
        mse = evaluator.evaluate(predictions)
        print(f"Mean Squared Error (MSE): {mse}")
        print("Batch processing completed for batch_id:", batch_id)
        
    parsedData.writeStream.foreachBatch(process_batch).trigger(processingTime="40 seconds").start()

    spark.streams.awaitAnyTermination()
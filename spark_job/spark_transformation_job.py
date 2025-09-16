import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys



logging.basicConfig(
    level =  logging.INFO,
    format="%(asctime)s - %(levelname)s -  %(message)s"
)

logger = logging.getLogger(__name__)





def main(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
    try:
        spark = SparkSession.builder \
            .appName("FlightBookingAnalysis") \
            .config("spark.sql.catalogImplementation", "hive") \
            .getOrCreate()
            
        logger.info("Spark Session Initialized")
        
        
        input_path = f"gs://airflow_project_gds/airflow_project-1/source-{env}"
        logger.info(f"Input path resolved: {input_path}")
        
        
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from GCS.")
        
        
        logger.info("Starting data transformations.")
        
        transformed_data = (
            data.withColumn(
            "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
            
        ).withColumn("lead_time_category", when(col("purchase_lead")< 7, lit("Last-Minute"))
                              .when((col("purchase_lead") >= 7) & (col("purchase_lead")<30), lit("short-Term"))
                               .otherwise(lit("Long-Term"))
            
        ).withColumn(
            "booking_sucesss_rate", expr("booking_complete / num_passenger")
        ))
        
        route_insights = transformed_data.groupby("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
            
        )
        
        
        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )
        
        logger.info("Data Transformation Completed!!")
        
        
        logger.info(f"Writing transformed data to BigQuery Table: {bq_project}:{bq_dataset}.{transformed_table}")
        
        transformed_data.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{transformed_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
            
        logger.info(f"Writing route insights to BigQuery table: {bq_project}:{bq_dataset}.{route_insights_table}")
        route_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}: {bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
            
            
        logger.info(f"Writing Booking origin insigts to BigQuery table: {bq_project}:{bq_dataset}.{origin_insights_table}")
        booking_origin_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
            
        logger.info("Data Written to BigQuery sucessfully")
        
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped!!")




if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Process flight Booking data and process write to BigQuery")
    parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
    parser.add_argument("--bq_project", required=True, help="BigQuery Project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery Dataset name")
    parser.add_argument("--transformed_table", required=True, help="BigQuery Table for transformed data")
    parser.add_argument("--route_insights_table", required=True, help="BigQuery table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="BigQuery table for booking origin insights")
    
    
    args = parser.parse_args()
    
    
    main (
        env = args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table    
    )
    


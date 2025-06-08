from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, sort_array, array_distinct, first
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, DoubleType

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName('scholarship_grouping') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark

def group_scholarships(input_file: str, output_file: str):
    """
    Transform origin_country into a list of countries while keeping all other data.
    This preserves the original data structure but consolidates the origin countries
    into a list for each unique scholarship.
    """
    spark = create_spark_session()
    
    try:
        # Read the parquet file
        df = spark.read.parquet(input_file)
        
        # List of columns that identify a unique scholarship
        key_columns = [
            'scholarship_name',
            'description',
            'program_country',
            'funding_info',
            'program_level',
            'required_level',
            'deadline',
            'intake',
            'exams',
            'fields_of_study',
            'duration',
            'documents',
            'website',
            'instructions'
        ]
        
        # First, get all columns except origin_country
        all_columns = df.columns
        non_key_columns = [col for col in all_columns if col not in key_columns and col != 'origin_country']
        
        # Group by key columns and aggregate
        grouped_df = df.groupBy(*key_columns) \
            .agg(
                array_distinct(sort_array(collect_list('origin_country'))).alias('origin_countries'),
                *[first(col).alias(col) for col in non_key_columns]
            )
        
        # Show some statistics
        total_original = df.count()
        total_grouped = grouped_df.count()
        duplicates = total_original - total_grouped
        
        print("\n=== Grouping Statistics ===")
        print(f"Original records: {total_original}")
        print(f"Unique scholarships: {total_grouped}")
        print(f"Duplicate entries (same scholarship, different countries): {duplicates}")
        
        # Show distribution of origin country counts
        print("\nDistribution of origin countries per scholarship:")
        grouped_df.select(
            'scholarship_name',
            'program_country',
            'origin_countries'
        ).orderBy('scholarship_name') \
        .show(5, truncate=False)
        
        # Save the transformed data
        print(f"\nSaving transformed data to {output_file}")
        grouped_df.write.mode("overwrite").parquet(output_file)
        
        # Validate the output
        print("\nValidating output file...")
        output_df = spark.read.parquet(output_file)
        print(f"Output file record count: {output_df.count()}")
        
        # Show schema of output file
        print("\nOutput file schema:")
        output_df.printSchema()
        
        return True
        
    except Exception as e:
        print(f"Error processing scholarships: {str(e)}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    input_file = '/Users/elnararb/Documents/UPC/Big_Data_Management/Project/20250524_221007_german_scholarships_trusted.parquet'
    output_file = 'scholarships_with_country_list.parquet'
    success = group_scholarships(input_file, output_file)
    
    if success:
        print("\nScholarship transformation completed successfully!")
    else:
        print("\nScholarship transformation failed!") 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start a Spark session
spark = SparkSession.builder.appName("PySpark Data Processing").getOrCreate()

# Load the dataset
file_path = "weight_change_dataset.csv"  # Replace with the correct file path if needed
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the first few rows of the dataset
print("Initial dataset:")
df.show(5)

# Example Data Transformation: Filter records where weight_change > 0 (assuming there's a 'weight_change' column)
transformed_df = df.filter(col("weight_change") > 0)

# Display transformed data
print("Transformed data (weight_change > 0):")
transformed_df.show(5)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("weight_data")

# Spark SQL Query: Select average weight_change grouped by another column (e.g., 'age_group')
# Adjust column names based on your dataset's actual structure
result_df = spark.sql("SELECT age_group, AVG(weight_change) AS avg_weight_change FROM weight_data GROUP BY age_group")

# Display the SQL query results
print("SQL Query Result (Average weight_change by age_group):")
result_df.show()

# Stop the Spark session
spark.stop()

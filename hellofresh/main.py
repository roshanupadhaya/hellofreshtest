from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, split

# Initialize Spark Session
spark = SparkSession.builder.appName("HelloFreshTest").getOrCreate()

# Load JSON files
df = spark.read.json("input/*.json")

# Preprocessing: Drop null values and duplicates
df_cleaned = df.dropna().dropDuplicates()

# Persist the pre-processed data as Parquet for optimized performance
df_cleaned.write.mode("overwrite").parquet("output/cleaned_data")

print("Task 1 completed: Data pre-processed and saved as Parquet.")

# Load the cleaned data
df_cleaned = spark.read.parquet("output/cleaned_data")

# Split ingredients column into an array of strings
df_cleaned = df_cleaned.withColumn("ingredients", split(col("ingredients"), ","))

# Extract recipes that include beef as an ingredient
df_beef = df_cleaned.filter(expr("array_contains(ingredients, 'beef')"))

# Calculate total cooking time and categorize by difficulty
df_with_time = df_beef.withColumn("total_cook_time", col("prepTime") + col("cookTime"))
df_with_time = df_with_time.withColumn(
    "difficulty",
    expr("""
        CASE 
            WHEN total_cook_time < 30 THEN 'easy'
            WHEN total_cook_time BETWEEN 30 AND 60 THEN 'medium'
            ELSE 'hard'
        END
    """)
)

# Calculate average cooking time per difficulty
df_avg_time = df_with_time.groupBy("difficulty").avg("total_cook_time")
df_avg_time = df_avg_time.withColumnRenamed("avg(total_cook_time)", "avg_total_cooking_time")

# Persist the final output as CSV
df_avg_time.write.mode("overwrite").csv("output/final_output", header=True)

print("Task 2 completed: Data analyzed and saved as CSV.")

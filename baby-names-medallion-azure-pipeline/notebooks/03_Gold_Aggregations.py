# Databricks notebook source
# Cell 1: Setup and load Silver data

from pyspark.sql.functions import col, sum, avg, count, rank, desc
from pyspark.sql.window import Window

storage_account = "babynamesadls123"
container = "gold"  # Note: saving to gold container
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# Configure storage access
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", 
               "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw==")

# Load Silver data
silver_path = "wasbs://silver@babynamesadls123.blob.core.windows.net/baby_names_silver"
df_silver = spark.read.format("delta").load(silver_path)

print(f"✅ Loaded {df_silver.count()} records from Silver layer")
print("\n📋 Silver schema:")
df_silver.printSchema()

display(df_silver.limit(5))

# COMMAND ----------

# Cell 2: Top names by decade (with rankings)

# Define window specification for ranking within each decade
window_spec = Window.partitionBy("decade").orderBy(desc("total_count"))

df_top_names = df_silver.groupBy("decade", "First_Name", "Sex") \
    .agg(
        sum("Count").alias("total_count"),
        count("*").alias("years_appeared"),
        avg("Count").alias("avg_per_year")
    ) \
    .withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") <= 5)  # Top 5 per decade

print("🏆 Top 5 names by decade:")
display(df_top_names.orderBy("decade", "rank"))

# Save to Gold layer
gold_top_path = f"{base_path}/top_names_by_decade"
df_top_names.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_top_path)

print(f"✅ Saved to: {gold_top_path}")

# COMMAND ----------

# Cell 1: Setup and load Silver data (with ALL imports)

from pyspark.sql.functions import col, sum, avg, count, countDistinct, rank, desc, min, max
from pyspark.sql.window import Window

storage_account = "babynamesadls123"
container = "gold"  # Note: saving to gold container
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# Configure storage access
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", 
               "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw==")

# Load Silver data
silver_path = "wasbs://silver@babynamesadls123.blob.core.windows.net/baby_names_silver"
df_silver = spark.read.format("delta").load(silver_path)

print(f"✅ Loaded {df_silver.count()} records from Silver layer")
print("\n📋 Silver schema:")
df_silver.printSchema()

display(df_silver.limit(5))

# COMMAND ----------

# Cell 3: Yearly trends

from pyspark.sql.functions import sum, countDistinct, avg

df_yearly = df_silver.groupBy("Year") \
    .agg(
        sum("Count").alias("total_babies"),
        countDistinct("First_Name").alias("unique_names"),
        avg("Count").alias("avg_per_name"),
        avg("name_length").alias("avg_name_length")
    ) \
    .orderBy("Year")

print("📈 Yearly trends:")
display(df_yearly)

# Save to Gold layer
gold_yearly_path = f"{base_path}/yearly_trends"
df_yearly.write.format("delta") \
    .mode("overwrite") \
    .save(gold_yearly_path)

print(f"✅ Saved to: {gold_yearly_path}")


# COMMAND ----------

# Cell 4: Gender trends over time

from pyspark.sql.functions import sum, countDistinct

df_gender = df_silver.groupBy("Year", "Sex") \
    .agg(
        sum("Count").alias("total"),
        countDistinct("First_Name").alias("unique_names")
    ) \
    .orderBy("Year", "Sex")

print("👥 Gender trends:")
display(df_gender)

# Save to Gold layer
gold_gender_path = f"{base_path}/gender_trends"
df_gender.write.format("delta") \
    .mode("overwrite") \
    .save(gold_gender_path)

print(f"✅ Saved to: {gold_gender_path}")

# COMMAND ----------

# Cell 5: Regional analysis

from pyspark.sql.functions import sum, countDistinct

df_region = df_silver.groupBy("region", "decade") \
    .agg(
        sum("Count").alias("total_babies"),
        countDistinct("First_Name").alias("unique_names")
    ) \
    .orderBy("region", "decade")

print("🗺️ Regional analysis:")
display(df_region)

# Save to Gold layer
gold_region_path = f"{base_path}/regional_summary"
df_region.write.format("delta") \
    .mode("overwrite") \
    .save(gold_region_path)

print(f"✅ Saved to: {gold_region_path}")

# COMMAND ----------

# Cell 6: Name popularity index (overall rankings)

from pyspark.sql.functions import sum, countDistinct, avg, desc
from pyspark.sql.window import Window

# Calculate overall popularity
df_name_rank = df_silver.groupBy("First_Name", "Sex") \
    .agg(
        sum("Count").alias("total_all_time"),
        countDistinct("Year").alias("years_active"),
        avg("Count").alias("avg_per_year")
    ) \
    .withColumn("popularity_rank", rank().over(Window.orderBy(desc("total_all_time")))) \
    .orderBy("popularity_rank")

print("📊 Name popularity index (top 20):")
display(df_name_rank.limit(20))

# Save to Gold layer
gold_names_path = f"{base_path}/name_popularity"
df_name_rank.write.format("delta") \
    .mode("overwrite") \
    .save(gold_names_path)

print(f"✅ Saved to: {gold_names_path}")

# COMMAND ----------

# Cell 7: Verify all Gold tables

print("🔍 Verifying Gold layer tables...")
print("=" * 50)

gold_tables = [
    ("Top names by decade", gold_top_path),
    ("Yearly trends", gold_yearly_path),
    ("Gender trends", gold_gender_path),
    ("Regional summary", gold_region_path),
    ("Name popularity", gold_names_path)
]

for name, path in gold_tables:
    try:
        df = spark.read.format("delta").load(path)
        print(f"\n✅ {name}: {df.count()} records")
        print(f"   Sample:")
        display(df.limit(2))
    except Exception as e:
        print(f"❌ {name}: Error - {e}")

# COMMAND ----------

# Cell 8: Create temporary views for SQL

# Load all Gold tables and create views
gold_top = spark.read.format("delta").load(gold_top_path)
gold_yearly = spark.read.format("delta").load(gold_yearly_path)
gold_gender = spark.read.format("delta").load(gold_gender_path)
gold_region = spark.read.format("delta").load(gold_region_path)
gold_names = spark.read.format("delta").load(gold_names_path)

gold_top.createOrReplaceTempView("gold_top_names")
gold_yearly.createOrReplaceTempView("gold_yearly_trends")
gold_gender.createOrReplaceTempView("gold_gender_trends")
gold_region.createOrReplaceTempView("gold_regional")
gold_names.createOrReplaceTempView("gold_name_popularity")

print("✅ Created temporary views for SQL queries")

# COMMAND ----------

# Cell 9: All SQL queries in one Python cell

print("📊 Top names overall:")
display(spark.sql("SELECT * FROM gold_name_popularity LIMIT 10"))

print("\n📈 Yearly trends:")
display(spark.sql("SELECT * FROM gold_yearly_trends ORDER BY Year"))

print("\n👤 Best decade for MARY:")
display(spark.sql("""
    SELECT decade, First_Name, total_count 
    FROM gold_top_names 
    WHERE First_Name = 'MARY'
    ORDER BY total_count DESC
"""))

print("\n⚥ Gender comparison over years:")
display(spark.sql("""
    SELECT Year, 
           MAX(CASE WHEN Sex = 'MALE' THEN total END) as male_babies,
           MAX(CASE WHEN Sex = 'FEMALE' THEN total END) as female_babies
    FROM gold_gender_trends
    GROUP BY Year
    ORDER BY Year
"""))

# COMMAND ----------

# Cell: Register Gold tables as temporary views

storage_account = "babynamesadls123"

# Define paths to your Gold tables
gold_paths = {
    "gold_top_names": f"wasbs://gold@{storage_account}.blob.core.windows.net/top_names_by_decade",
    "gold_yearly_trends": f"wasbs://gold@{storage_account}.blob.core.windows.net/yearly_trends",
    "gold_gender_trends": f"wasbs://gold@{storage_account}.blob.core.windows.net/gender_trends",
    "gold_regional": f"wasbs://gold@{storage_account}.blob.core.windows.net/regional_summary",
    "gold_name_popularity": f"wasbs://gold@{storage_account}.blob.core.windows.net/name_popularity"
}

# Load each table and create a temporary view
for view_name, path in gold_paths.items():
    try:
        df = spark.read.format("delta").load(path)
        df.createOrReplaceTempView(view_name)
        count = df.count()
        print(f"✅ Created view '{view_name}' with {count} records from {path}")
    except Exception as e:
        print(f"❌ Failed to load {view_name}: {e}")

print("\n🎯 All Gold tables are now available as temporary views!")
print("You can now run your SQL queries using these view names.")

# COMMAND ----------

# Cell: Create Tables Using Spark DataFrame API (Bypasses Unity Catalog)

storage_account = "babynamesadls123"
storage_key = "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw=="

# Set config for this session
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)

# Define paths and table names
gold_tables = [
    ("gold_top_names", f"wasbs://gold@{storage_account}.blob.core.windows.net/top_names_by_decade"),
    ("gold_yearly_trends", f"wasbs://gold@{storage_account}.blob.core.windows.net/yearly_trends"),
    ("gold_gender_trends", f"wasbs://gold@{storage_account}.blob.core.windows.net/gender_trends"),
    ("gold_regional", f"wasbs://gold@{storage_account}.blob.core.windows.net/regional_summary"),
    ("gold_name_popularity", f"wasbs://gold@{storage_account}.blob.core.windows.net/name_popularity")
]

for table_name, path in gold_tables:
    try:
        # Read the Delta table using Spark
        df = spark.read.format("delta").load(path)
        # Drop existing table if any
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        # Save as a managed table (data will be copied, not referenced externally)
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"✅ Created managed table '{table_name}' with {df.count()} records")
    except Exception as e:
        print(f"❌ Failed for {table_name}: {e}")
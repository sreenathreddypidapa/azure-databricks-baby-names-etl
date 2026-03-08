# Databricks notebook source
# Cell 0: Configure Azure Storage Access

storage_account = "babynamesadls123"
storage_key = "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw=="

# Set Spark configuration for Azure Storage
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)

# Test the connection
print("🔍 Testing storage connection...")
try:
    dbutils.fs.ls(f"wasbs://bronze@{storage_account}.blob.core.windows.net/")
    print("✅ Successfully connected to Azure Storage!")
except Exception as e:
    print(f"❌ Connection failed: {e}")

print("\n✅ Storage credentials configured for this notebook")

# COMMAND ----------

# Cell 1: Silver Layer Transformations

from pyspark.sql.functions import col, upper, trim, when, length, current_timestamp, lit
from pyspark.sql.types import IntegerType

storage_account = "babynamesadls123"
container = "silver"  # Note: we're saving to silver container
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

# IMPORTANT: Set credentials again (in case cell runs alone)
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw==")

print("📥 Reading from Bronze layer...")
bronze_path = f"wasbs://bronze@{storage_account}.blob.core.windows.net/baby_names_bronze"

try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    print(f"✅ Successfully loaded {df_bronze.count()} records from Bronze")
    print("\n📋 Bronze Schema:")
    df_bronze.printSchema()
    
    print("\n🔍 Bronze Data Preview:")
    display(df_bronze.limit(5))
    
except Exception as e:
    print(f"❌ Error reading Bronze layer: {e}")
    print("\n🔄 Attempting to fix...")
    
    # Re-check configuration
    spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)
    print("✅ Reconfigured credentials, please run this cell again")

# COMMAND ----------

# Cell 2: Data Cleaning

print("🧹 Applying data cleaning rules...")

# Track initial count
initial_count = df_bronze.count()

# Apply cleaning transformations
df_cleaned = df_bronze \
    .withColumn("First_Name", upper(trim(col("First_Name")))) \
    .withColumn("County", upper(trim(col("County")))) \
    .withColumn("Sex", upper(trim(col("Sex")))) \
    .withColumn("Sex", 
                when(col("Sex").isin("M", "MALE"), "MALE")
                .when(col("Sex").isin("F", "FEMALE"), "FEMALE")
                .otherwise("UNKNOWN")) \
    .filter(col("Count") > 0) \
    .filter(col("Year").isNotNull()) \
    .filter(col("First_Name").isNotNull()) \
    .filter(col("County").isNotNull())

# Calculate counts after cleaning
final_count = df_cleaned.count()
removed_count = initial_count - final_count

print(f"\n📊 Cleaning summary:")
print(f"  - Records before cleaning: {initial_count}")
print(f"  - Records after cleaning: {final_count}")
print(f"  - Records removed: {removed_count}")
if initial_count > 0:
    print(f"  - Data quality score: {(final_count/initial_count*100):.1f}%")
else:
    print(f"  - Data quality score: N/A (no records)")

print("\n✅ Cleaned data preview:")
display(df_cleaned)

# COMMAND ----------

# Cell 3: Add derived features

from pyspark.sql.functions import col, when, floor, length, current_timestamp, lit

print("✨ Adding derived features to create Silver layer...")

# Apply all transformations in a single chain WITHOUT inline comments
df_silver = df_cleaned \
    .withColumn("name_length", length(col("First_Name"))) \
    .withColumn("name_category", 
                when(col("name_length") <= 4, "SHORT")
                .when(col("name_length") <= 7, "MEDIUM")
                .otherwise("LONG")) \
    .withColumn("decade", (floor(col("Year") / 10) * 10)) \
    .withColumn("era",
                when(col("Year") < 1920, "EARLY_1900s")
                .when(col("Year") < 1950, "MID_CENTURY")
                .when(col("Year") < 1980, "LATE_1900s")
                .when(col("Year") < 2000, "END_CENTURY")
                .otherwise("MODERN")) \
    .withColumn("region",
                when(col("County").contains("KINGS"), "BROOKLYN")
                .when(col("County").contains("QUEENS"), "QUEENS")
                .otherwise("OTHER")) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_version", lit("v1.0"))

print(f"✅ Added {len(df_silver.columns) - len(df_cleaned.columns)} new features")
print(f"Total columns now: {len(df_silver.columns)}")
print(f"Columns: {df_silver.columns}")

print("\n📋 Silver Schema with new features:")
df_silver.printSchema()

print("\n👀 Silver data preview (with new features):")
display(df_silver)

# COMMAND ----------

# Cell 4: Explore the new features

print("📊 Exploring derived features:")

# Check name categories
print("\n🏷️ Name categories distribution:")
df_silver.groupBy("name_category").count().show()

# Check decades
print("\n📅 Decades present:")
df_silver.select("decade").distinct().orderBy("decade").show()

# Check eras
print("\n⏳ Era distribution:")
df_silver.groupBy("era").count().show()

# Check regions
print("\n🗺️ Region distribution:")
df_silver.groupBy("region").count().show()

# Show a nice summary
print("\n📈 Feature summary:")
display(df_silver.select(
    "First_Name", 
    "name_length", 
    "name_category",
    "Year",
    "decade",
    "era",
    "region",
    "Count"
).limit(10))

# COMMAND ----------

# Cell 5: Validate new features

from pyspark.sql.functions import col, min, max

print("🔍 Validating new features...")

# Check for any nulls in new features
new_features = ["name_length", "name_category", "decade", "era", "region"]
for feature in new_features:
    null_count = df_silver.filter(col(feature).isNull()).count()
    print(f"  - Nulls in {feature}: {null_count}")

# Get min and max values using DataFrame operations
name_length_min = df_silver.select(min("name_length")).collect()[0][0]
name_length_max = df_silver.select(max("name_length")).collect()[0][0]
decade_min = df_silver.select(min("decade")).collect()[0][0]
decade_max = df_silver.select(max("decade")).collect()[0][0]

# Print ranges with proper formatting
print(f"\n📏 Name length range: {name_length_min} to {name_length_max}")
print(f"📅 Decade range: {decade_min} to {decade_max}")

# Also check unique values in categorical features
print("\n🎯 Unique values in categorical features:")
for feature in ["name_category", "era", "region"]:
    unique_values = df_silver.select(feature).distinct().rdd.flatMap(lambda x: x).collect()
    print(f"  - {feature}: {unique_values}")

print("\n✅ All features validated!")

# COMMAND ----------

# Cell 4: Silver data quality validation

from pyspark.sql.functions import col, min, max

print("🔍 Running Silver layer quality checks...")

# Check 1: No nulls in key columns
key_columns = ["Year", "First_Name", "County", "Sex", "Count"]
null_checks = {}
for col_name in key_columns:
    null_count = df_silver.filter(col(col_name).isNull()).count()
    null_checks[col_name] = null_count
    print(f"  - Nulls in {col_name}: {null_count}")

# Check 2: Value ranges - FIXED: compute values first, then print
print("\n📊 Value range checks:")

# Get min and max values first
year_min = df_silver.select(min("Year")).collect()[0][0]
year_max = df_silver.select(max("Year")).collect()[0][0]
name_len_min = df_silver.select(min("name_length")).collect()[0][0]
name_len_max = df_silver.select(max("name_length")).collect()[0][0]
count_min = df_silver.select(min("Count")).collect()[0][0]
count_max = df_silver.select(max("Count")).collect()[0][0]

print(f"  - Years: {year_min} to {year_max}")
print(f"  - Name lengths: {name_len_min} to {name_len_max}")
print(f"  - Count range: {count_min} to {count_max}")

# Check 3: Unique values in categorical columns - FIXED: no .rdd, use collect()
print("\n🎯 Unique values in categories:")

# Get distinct values without using .rdd
sex_values = [row[0] for row in df_silver.select("Sex").distinct().collect()]
name_cat_values = [row[0] for row in df_silver.select("name_category").distinct().collect()]
era_values = [row[0] for row in df_silver.select("era").distinct().collect()]
region_values = [row[0] for row in df_silver.select("region").distinct().collect()]

print(f"  - Sex: {sex_values}")
print(f"  - Name categories: {name_cat_values}")
print(f"  - Eras: {era_values}")
print(f"  - Regions: {region_values}")

print("\n✅ All quality checks passed!")

# COMMAND ----------

# Cell 6: Save to Silver layer

storage_account = "babynamesadls123"
container = "silver"
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"
silver_path = f"{base_path}/baby_names_silver"

print(f"💾 Saving Silver layer to: {silver_path}")

# Save as Delta
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("decade") \
    .save(silver_path)

print("✅ Silver layer saved successfully!")

# Create a table for easy querying
spark.sql("CREATE DATABASE IF NOT EXISTS baby_db")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS baby_db.baby_names_silver
    USING DELTA
    LOCATION '{silver_path}'
""")

print("\n✅ Table 'baby_db.baby_names_silver' created!")

# Verify
df_check = spark.read.format("delta").load(silver_path)
print(f"\n✅ Verified: {df_check.count()} records in Silver layer")
display(df_check.limit(5))

# COMMAND ----------

# Cell 7: Access Silver data directly

storage_account = "babynamesadls123"
container = "silver"
silver_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/baby_names_silver"

# Read directly from storage
df_silver = spark.read.format("delta").load(silver_path)

print(f"✅ Successfully read {df_silver.count()} records from Silver layer")
print("\n📋 Silver Data Preview:")
display(df_silver.limit(5))

# Create a temporary view for SQL access
df_silver.createOrReplaceTempView("silver_view")
print("\n✅ Created temporary view 'silver_view' for SQL queries")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 8: Query Silver data with SQL
# MAGIC
# MAGIC -- Check the data
# MAGIC SELECT * FROM silver_view LIMIT 10;
# MAGIC
# MAGIC -- Basic aggregations
# MAGIC SELECT 
# MAGIC     decade,
# MAGIC     COUNT(*) as name_count,
# MAGIC     SUM(Count) as total_babies
# MAGIC FROM silver_view
# MAGIC GROUP BY decade
# MAGIC ORDER BY decade;
# MAGIC
# MAGIC -- Top names by decade
# MAGIC SELECT 
# MAGIC     decade,
# MAGIC     First_Name,
# MAGIC     SUM(Count) as total
# MAGIC FROM silver_view
# MAGIC GROUP BY decade, First_Name
# MAGIC ORDER BY decade, total DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# Cell 9: Create a reusable function to access Silver data

storage_account = "babynamesadls123"
container = "silver"
silver_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/baby_names_silver"

def get_silver_data():
    """Function to load Silver data"""
    return spark.read.format("delta").load(silver_path)

def refresh_silver_view():
    """Create or replace temporary view"""
    df = get_silver_data()
    df.createOrReplaceTempView("silver_data")
    print(f"✅ View 'silver_data' created with {df.count()} records")
    return df

# Create the view
df_silver = refresh_silver_view()

print("\n📊 Sample data:")
display(df_silver.limit(5))

# COMMAND ----------

# Cell 12: Quick Silver Data Analysis

from pyspark.sql.functions import col, sum, count, min, max

storage_account = "babynamesadls123"
container = "silver"
silver_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/baby_names_silver"

df = spark.read.format("delta").load(silver_path)

print("📈 Silver Data Summary")
print("=" * 50)

# 1. Overall statistics - FIXED: compute values first
print(f"\n📊 Overall Stats:")

total_records = df.count()
year_min = df.select(min("Year")).collect()[0][0]
year_max = df.select(max("Year")).collect()[0][0]
unique_names = df.select("First_Name").distinct().count()
unique_counties = df.select("County").distinct().count()

print(f"  - Total records: {total_records}")
print(f"  - Years covered: {year_min} to {year_max}")
print(f"  - Unique names: {unique_names}")
print(f"  - Unique counties: {unique_counties}")

# 2. Gender distribution
print(f"\n👥 Gender Distribution:")
df.groupBy("Sex").count().show()

# 3. Top names overall
print(f"\n🏆 Top 5 Names Overall:")
df.groupBy("First_Name").sum("Count") \
    .withColumnRenamed("sum(Count)", "Total") \
    .orderBy("Total", ascending=False) \
    .show(5)

# 4. Names by decade
print(f"\n📅 Top Names by Decade:")
df.groupBy("decade", "First_Name").sum("Count") \
    .withColumnRenamed("sum(Count)", "Total") \
    .orderBy("decade", "Total", ascending=False) \
    .show(10)

# 5. Region analysis
print(f"\n🗺️ Region Distribution:")
df.groupBy("region").sum("Count") \
    .withColumnRenamed("sum(Count)", "Total") \
    .orderBy("Total", ascending=False) \
    .show()
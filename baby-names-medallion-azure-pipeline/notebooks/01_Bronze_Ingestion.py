# Databricks notebook source
# Cell 1: Complete ADLS Gen2 Access Setup

storage_account = "babynamesadls123"
container = "bronze"
storage_key = "JEqZI8A7KKtxG/ywsZUETIQAbdtU1FuhbBsqUZg41tbywKW+nmO5XZfqzYLU8YML3Ul94Zhh9eoM+AStqi9krw=="

print("🔧 Step 1: Clearing any existing configurations...")
# Clear any conflicting configs
try:
    spark.conf.unset(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net")
    spark.conf.unset(f"fs.azure.account.key.{storage_account}.blob.core.windows.net")
except:
    pass

print("✅ Step 2: Setting storage account key...")
# Set the configuration using the correct format
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)

print("✅ Step 3: Testing connection...")
# Test connection using different formats
paths_to_test = [
    f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    f"wasbs://{container}@{storage_account}.blob.core.windows.net/",
]

connection_successful = False
for path in paths_to_test:
    print(f"\n📂 Testing: {path}")
    try:
        # Try to list files
        files = dbutils.fs.ls(path)
        print(f"✅ SUCCESS! Connected to {path}")
        print(f"Found {len(files)} items:")
        for f in files:
            print(f"  - {f.name}")
        connection_successful = True
        working_path = path
        break
    except Exception as e:
        print(f"❌ Failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")

if connection_successful:
    print(f"\n🎉 Successfully connected to ADLS!")
    print(f"Working path: {working_path}")
    
    # Now let's create a simple test file
    print("\n📝 Creating test file...")
    test_df = spark.range(10)
    test_path = working_path + "test_folder/"
    test_df.write.mode("overwrite").parquet(test_path + "test_file.parquet")
    print(f"✅ Test file written to: {test_path}")
    
    # Verify it worked
    print("\n🔍 Reading test file back...")
    read_df = spark.read.parquet(test_path + "test_file.parquet")
    print(f"Read {read_df.count()} rows successfully!")
    
else:
    print("\n❌ Could not connect to storage. Let's check the container...")
    
    # Check if container exists using HTTP
    import requests
    url = f"https://{storage_account}.blob.core.windows.net/{container}?restype=container"
    response = requests.get(url)
    print(f"\nHTTP Check - Status code: {response.status_code}")
    if response.status_code == 404:
        print(f"❌ Container '{container}' does not exist!")
        print("Please create it in Azure Portal:")
        print("1. Go to your storage account")
        print("2. Click 'Containers'")
        print("3. Click '+ Container'")
        print(f"4. Name it: {container}")
    elif response.status_code == 403:
        print("✅ Container exists but requires authentication (this is normal)")

# COMMAND ----------

# Cell 6: Create a sample baby names dataset manually

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("First_Name", StringType(), True),
    StructField("County", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Count", IntegerType(), True)
])

# Create sample data
sample_data = [
    (1910, "Mary", "KINGS", "F", 250),
    (1910, "John", "KINGS", "M", 200),
    (1910, "Helen", "QUEENS", "F", 180),
    (1911, "Mary", "KINGS", "F", 265),
    (1911, "William", "KINGS", "M", 195),
    (1911, "Anna", "QUEENS", "F", 170),
    (1912, "John", "KINGS", "M", 220),
    (1912, "Mary", "QUEENS", "F", 240),
    (1912, "Robert", "KINGS", "M", 190),
    (1913, "Mary", "KINGS", "F", 280),
]

df_raw = spark.createDataFrame(sample_data, schema)

print("✅ Created sample baby names dataset")
print(f"Records: {df_raw.count()}")
display(df_raw)

# COMMAND ----------

# Cell 8: Complete Bronze Layer Ingestion (CORRECTED)

from pyspark.sql.functions import current_timestamp, lit, col

storage_account = "babynamesadls123"
container = "bronze"
base_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

print("📊 Working with sample baby names dataset")
print(f"Records: {df_raw.count()}")

print("\n🔍 Exploring the data:")
print("Schema:")
df_raw.printSchema()

print("\nData preview:")
display(df_raw)

# Check data quality - FIXED SYNTAX
print("\n🔎 Data quality check:")

# Method 1: Using count with filter (cleaner syntax)
null_count = df_raw.filter(
    (col("Year").isNull()) | 
    (col("First_Name").isNull()) | 
    (col("County").isNull()) | 
    (col("Sex").isNull()) | 
    (col("Count").isNull())
).count()
print(f"Null values in any column: {null_count}")

negative_count = df_raw.filter(col("Count") < 0).count()
print(f"Negative counts: {negative_count}")

# Add Bronze layer metadata
print("\n✨ Adding Bronze layer metadata...")
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("manual_sample")) \
    .withColumn("data_quality_passed", lit(True)) \
    .withColumn("pipeline_version", lit("v1.0"))

print("✅ Metadata columns added:")
display(df_bronze.limit(5))

# Save to Bronze layer
bronze_path = f"{base_path}/baby_names_bronze"
print(f"\n💾 Saving to Bronze layer at: {bronze_path}")

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.columnMapping.mode", "name") \
    .save(bronze_path)

print("✅ Bronze layer saved successfully!")

# Verify the save
print("\n🔍 Verifying saved data...")
df_verified = spark.read.format("delta").load(bronze_path)
print(f"Verified {df_verified.count()} records in Bronze layer")
print(f"Columns: {df_verified.columns}")

print("\n📋 Sample of saved data:")
display(df_verified.limit(5))

# Create a table for easier SQL access
print("\n📊 Registering as SQL table...")
df_verified.createOrReplaceTempView("baby_names_bronze_temp")
spark.sql("CREATE DATABASE IF NOT EXISTS baby_db")
spark.sql("DROP TABLE IF EXISTS baby_db.baby_names_bronze")
spark.sql(f"CREATE TABLE baby_db.baby_names_bronze USING DELTA LOCATION '{bronze_path}'")

print("✅ Table 'baby_db.baby_names_bronze' created!")
print("\nYou can now query with SQL:")
print("%sql")
print("SELECT * FROM baby_db.baby_names_bronze LIMIT 10;")

# COMMAND ----------

# Cell 9: Access Bronze data directly

storage_account = "babynamesadls123"
container = "bronze"
bronze_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/baby_names_bronze"

# Read directly from storage
df_bronze = spark.read.format("delta").load(bronze_path)

print(f"✅ Successfully read {df_bronze.count()} records from Bronze layer")
print("\n📋 Data Preview:")
display(df_bronze)

# Create a temporary view for SQL access (not persistent)
df_bronze.createOrReplaceTempView("bronze_view")

print("\n✅ Created temporary view 'bronze_view' for SQL queries")
# Baby Names ETL Pipeline - Azure Databricks Project

## 📋 Project Overview
An end-to-end data pipeline implementing the medallion architecture (Bronze-Silver-Gold) on Azure Databricks using the Baby Names dataset. This project demonstrates a complete data engineering workflow from raw data ingestion to interactive business dashboards.

##  Architecture

BRONZE LAYER │ Raw data ingestion with metadata
▼

SILVER LAYER │ Cleaned data with engineered features
▼

GOLD LAYER │ Business-ready aggregations
▼

DASHBOARD │ Interactive visualizations


text

## Technologies Used
| Technology | Purpose |
|------------|---------|
| **Azure Databricks** | Data processing platform |
| **Apache Spark** | Distributed computing engine |
| **Delta Lake** | ACID transactions on data lake |
| **Azure Data Lake Storage Gen2** | Cloud storage |
| **PySpark** | Python API for Spark |
| **Databricks SQL** | Dashboard visualizations |

## Dataset
- **Source**: NYC Baby Names sample dataset
- **Records**: 10 records (demonstration)
- **Years**: 1910-1913
- **Features**: Year, First_Name, County, Sex, Count

## Setup Instructions

### Prerequisites
- Azure account (with $200 free credits)
- Azure Databricks workspace
- Azure Data Lake Storage Gen2 account

### Step 1: Clone the Repository

git clone https://github.com/sreenathreddypidapa/sreenathreddypidapa-azure-databricks-baby-names-etl.git
cd azure-databricks-baby-names-etl

### Step 2: Configure Azure Resources
Create a Resource Group: baby-names-rg
Create Storage Account: babynamesadls
Enable hierarchical namespace
Create containers: bronze, silver, gold
Create Databricks Workspace: baby-names-databricks

### Step 3: Import Notebooks
Import the notebooks in this order:

01_Bronze_Ingestion.py

02_Silver_Transformation.py

03_Gold_Aggregations.py

Step 4: Configure Storage Access
In each notebook, add your storage key:

python
storage_account = "babynamesadls123"
storage_key = "your-storage-key-here"
spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_key)

Step 5: Run the Pipeline
Execute notebooks sequentially. The cluster will auto-terminate after 120 minutes of inactivity.

Key Insights
Insight	Finding
Top Female Name	MARY (1,035 in 1910)
Top Male Name	JOHN (420 in 1910)
Gender Distribution	~63% Female, ~37% Male
Name Diversity	Increasing over time
Regional Leader	BROOKLYN

🎯 Bronze Layer
Raw data ingestion from Azure Data Lake Storage Gen2

Added metadata: ingestion_timestamp, source_file

Delta Lake format with ACID transactions

Silver Layer
Data cleaning and standardization

Feature engineering:

name_length, name_category

decade calculation

era classification (EARLY_1900s, MID_CENTURY, etc.)

region mapping

Data quality validation (null checks, value ranges)

Gold Layer Tables
Table	Purpose	Records
gold_top_names	Ranked names by decade	5
gold_yearly_trends	Year-over-year metrics	4
gold_gender_trends	Male vs female analysis	7
gold_regional	Geographic distribution	2
gold_name_popularity	All-time rankings	6
🎯 Dashboard Visualizations
Top Names by Decade: Bar chart showing top 5 names per decade

Yearly Trends: Line chart tracking total babies and unique names over time

Gender Distribution: Pie chart showing male vs female distribution

Regional Analysis: Bar chart of name counts by region

Name Popularity: Bar chart of all-time top 10 names

📁 Repository Structure
text
├── notebooks/
│   ├── 01_Bronze_Ingestion.py      # Raw data ingestion
│   ├── 02_Silver_Transformation.py # Data cleaning & features
│   └── 03_Gold_Aggregations.py     # Business aggregations
├── dashboard/
│   └── Baby_Names_Dashboard.html   # Interactive dashboard
├── screenshots/
│   ├── bronze_layer.png
│   ├── silver_layer.png
│   ├── gold_layer.png
│   └── dashboard.png
└── README.md
🧪 Sample SQL Queries
sql
-- Top names by decade
SELECT decade, First_Name, total_count
FROM gold_top_names
WHERE rank <= 5
ORDER BY decade, rank;

-- Yearly trends
SELECT Year, total_babies, unique_names
FROM gold_yearly_trends
ORDER BY Year;

-- Gender distribution
SELECT Sex, SUM(total) as total_babies
FROM gold_gender_trends
GROUP BY Sex;

-- Regional analysis
SELECT region, SUM(total_babies) as total_babies
FROM gold_regional
GROUP BY region
ORDER BY total_babies DESC;

-- Top 10 names all-time
SELECT First_Name, total_all_time
FROM gold_name_popularity
ORDER BY popularity_rank
LIMIT 10;
 Cost Optimization
Cluster auto-termination: 120 minutes of inactivity

Single node cluster: Used for development

Resource cleanup: Stop/delete resources when not in use

Estimated cost: ~$3-5 from $200 credits

🎯 Learning Outcomes
* Azure resource provisioning
* Data Lake Storage Gen2 configuration
* Databricks workspace and cluster setup
* Medallion architecture implementation
* PySpark transformations
* Delta Lake format
* Data quality validation
* Feature engineering
* Gold layer aggregations
* Dashboard creation

📝 Future Enhancements
Ingest full dataset (more years)

Add automated scheduling

Implement CI/CD with Databricks Asset Bundles

Add unit tests for data quality

Connect to Power BI for advanced visualizations

📧 Contact
[Sreenath Reddy Pidapa] - [sreenathreddypidapa@gmail.com]

Project Link: [https://github.com/sreenathreddypidapa/azure-databricks-baby-names-etl]

🙏 Acknowledgments
Azure Databricks documentation
NYC Open Data
Microsoft Learn modules

# United States Immigration Data Analysis
### Data Engineering Capstone Project

#### Project Summary
This project’s objective was to establish an ETL pipeline for I94 immigration, earth surface temperatures, airport codes, and city demographic datasets to create an analytics data warehouse for US immigration trends.

The tools utilized for this project include **Apache Spark and AWS S3**. Spark will be used to read and process the data and S3 will store the fact and dimension tables created with a Snowflake architecture. 


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


### Directory
 - city_demo_data: Contains U.S. Cities Demographics CSV file
 - i94_sas_label_data: Contains pre-cleaned CSV files created from SAS_Labels_Descriptions file
 - sas_data: Contains I94 Immigration Data parquet files
 - [Capstone_Project.ipynb](Capstone_Project.ipynb): Python 3 Jupyter Notebook that details project and process
 - [config.cfg](config.cfg): Configuration file for AWS credentials
 - [data_dictionary.md](data_dictionary.md): Data dictionary for all tables in data warehouse
 - [elt.py](elt.py): Script to build the data pipeline and create the data model
 - [Immigration_Data_Model.jpg](Immigration_Data_Model.jpg): Data model for Immigration data warehouse
 - [quality_checks.py](quality_checks.py): Script to run the quality checks for the data model
 - [README.md](README.md): Project Intro and Directory
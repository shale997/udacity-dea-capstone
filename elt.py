import configparser
import datetime as dt
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date, date_format, avg
from pyspark.sql.functions import year, date_format, monotonically_increasing_id
from pyspark.sql.types import DoubleType
import quality_checks


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create a new spark session
    
    Parameters: none
    Return: Spark connection
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_i94_labels(spark, output_data, dataframes):
    """
    Read pre-cleaned csv files containing i94 SAS Label Descriptions and load it into a dataframe 
    that will extract columns and create the country, city, state and visa dimension tables.
    Writes parquet files to an S3 bucket.
    
    Parameters: 
     - spark: Spark session previously created
     - output_data: Path to the S3 bucket where the files are being written
     - dataframes: Dictionary of dataframes
    Return: dataframes
    """
    # read i94 countries data file
    df_countries = spark.read.csv("i94_sas_label_data/i94_countries.csv", header=True)
    print("read countries data")
    
    # extract columns to create dim_country table
    dim_country_table = df_countries.select(
                                    'country_cd',
                                    'country_name'
                            ).dropDuplicates()
    
    # write dim_country table to parquet files
    dim_country_table.write.mode('overwrite') \
                        .parquet(os.path.join(output_data, 'dim_country.parquet'))
    
    print("countries data has been written")
    dataframes["dim_country_table"] = dim_country_table
    
    
    # read i94 cities data file
    df_cities = spark.read.csv("i94_sas_label_data/i94_cities.csv", header=True)
    print("read cities data")
    
    # extract columns to create dim_city table
    dim_city_table = df_cities.select(
                                    'city_cd',
                                    'city_name'
                            ).dropDuplicates()
    
    # write dim_city table to parquet files
    dim_city_table.write.mode('overwrite') \
                        .parquet(os.path.join(output_data, 'dim_city.parquet'))

    print("cities data has been written")
    dataframes["dim_city_table"] = dim_city_table
    
    
    # read i94 states data file
    df_states = spark.read.csv("i94_sas_label_data/i94_states.csv", header=True)
    print("read states data")
    
    # extract columns to create dim_state table
    dim_state_table = df_states.select(
                                    'state_cd',
                                    'state_name'
                            ).dropDuplicates()
    
    # write dim_state table to parquet files
    dim_state_table.write.mode('overwrite') \
                        .parquet(os.path.join(output_data, 'dim_state.parquet'))
    
    print("states data has been written")
    dataframes["dim_state_table"] = dim_state_table
    
    
    # read i94 visas data file
    df_visas = spark.read.csv("i94_sas_label_data/i94_visas.csv", header=True)
    print("read visas data")
    
    # extract columns to create dim_visa table
    dim_visa_table = df_visas.select(
                                    'visa_cd',
                                    'visa_desc'
                            ).dropDuplicates()
    
    # write dim_visa table to parquet files
    dim_visa_table.write.mode('overwrite') \
                        .parquet(os.path.join(output_data, 'dim_visa.parquet'))
    
    print("visas data has been written")
    dataframes["dim_visa_table"] = dim_visa_table
    
    return dataframes

    
    
def process_city_demographics_data(spark, output_data, dataframes):
    """
    Read City Demographics and Global Land Temperature data and load it into a dataframe that 
    will extract columns and create the city_demographics dimension table.
    Writes parquet files to an S3 bucket.
    
    Parameters: 
     - spark: Spark session previously created
     - output_data: Path to the S3 bucket where the files are being written
     - dataframes: Dictionary of dataframes
    Return: dataframes
    """
    # read US cities demographics data file
    df_demog = spark.read.format('csv').options(header=True, delimiter=';').load("city_demo_data/us-cities-demographics.csv")
    print("read demographics data")
    
    # read Global Land Temperature data file
    df_temp = spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv', header=True)
    print("read temperature data")
    
    # clean Global Land Temperature data
    df_temp = clean_temperature_data(df_temp)
    print("temperature data has been cleaned")
    
    # extract columns to create dim_city_demographics
    dim_city_demographics_table = df_demog.join(df_temp, (df_demog.City == df_temp.t_city), 'left_outer') \
                                .select(
                                        col("City").alias('city'),
                                        col("State Code").alias('state_cd'),
                                        col("Median Age").alias('median_age'),
                                        col("Male Population").alias('male_pop'),
                                        col("Female Population").alias('female_pop'),
                                        col("Total Population").alias('total_pop'),
                                        col("Foreign-born").alias('foreign_born_pop'),
                                        col("Average Household Size").alias('avg_hh_size'),
                                        df_temp.avg_temperature) \
                                .dropDuplicates()
    
    
    
    # write dim_city_demographics
    dim_city_demographics_table.write.mode('overwrite').partitionBy('state_cd') \
                        .parquet(os.path.join(output_data, 'dim_city_demographics.parquet'))

    print("demographics data has been written")
    dataframes["dim_city_demographics_table"] = dim_city_demographics_table
    
    return dataframes
    
    
def clean_temperature_data(df_temp):
    """
    Clean the data by doing the following:
        1. Limit the dataset to only include data from the U.S.
        2. Convert dt string into date format and extract the year
        3. Limit the dataset to include only data from the latest year in the dataset (2013)
        4. Convert AverageTemperature to a double value for aggregation
        5. Calculate the average temperature and convert the temperature from Celsius to Fahrenheit
        
    Parameters: 
     - df_temp: Global Land Temperate dataframe
    Return: cleaned temperature dataframe
    """
    print("temperature data is being cleaned")
    # filter to temperate data to only include the United States
    df_temp_us = df_temp.where(df_temp['Country'] == 'United States')
    
    # convert the date to be stored in 'YYYY' format and filter to the most recent year, 2013
    df_temp_us_lastest = df_temp_us.withColumn('dt_year', year(to_date(df_temp_us.dt)))
    df_temp_us_lastest = df_temp_us_lastest.where(df_temp_us_lastest['dt_year'] == 2013)
    
    # convert AverageTemperature to double for aggregation
    df_temp_agg = df_temp_us_lastest.withColumn('AverageTemperature', df_temp_us_lastest.AverageTemperature.cast('double'))
    
    # get the AverageTemperature for each city
    df_temp_agg = df_temp_agg.groupBy('City').avg('AverageTemperature')
    df_temp_agg = df_temp_agg.withColumnRenamed('avg(AverageTemperature)', 'avg_temperature')
    df_temp_agg = df_temp_agg.withColumnRenamed('City', 't_city')
    
    # convert fahrenheit to celsius using a UDF
    get_fahrenheit = udf(lambda x: convertCelsiusToFahrenheit(x), DoubleType())
    df_temp_agg = df_temp_agg.withColumn('avg_temperature', get_fahrenheit(df_temp_agg.avg_temperature))
    
    return df_temp_agg



def convertCelsiusToFahrenheit(temperature):
    """
    Converts input temperature from Celsius to Fahrenheit
    
    Parameter:
     - temperature: Input temperature in Celsius
    Return: Input temperature in Fahrenheit
    """
    print("average temperature is being converted into fahrenheit")
    temperature = (temperature*1.8) + 32
    return temperature



def process_immigration_data(spark, output_data, dataframes):
    """
    Read sas_data containing I94 Immigration data and loads it into a dataframe that 
    will extract columns and create the immigration_i94 fact table.
    Writes parquet files to an S3 bucket.
    
    Parameters: 
     - spark: Spark session previously created
     - output_data: Path to the S3 bucket where the files are being written
     - dataframes: Dictionary of dataframes
    Return: dataframes
    """
    # read i94 countries data file
    df_immigration = spark.read.parquet("sas_data")
    print("read immigration data")
    
    # udf to convert SAS date format to datetime and store it as a string
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    
    # convert arrdate and depdate to datetime
    df_immigration = df_immigration.withColumn('arrdate', get_date(df_immigration.arrdate))
    df_immigration = df_immigration.withColumn('depdate', get_date(df_immigration.depdate))
    
    
    # extract columns to create dim_country table
    fact_immigration_94_table = df_immigration.select(
                                    col("cicid").alias('cic_id'),
                                    col("i94yr").alias('i94_year'),
                                    col("i94mon").alias('i94_month'),
                                    col("i94cit").alias('i94_cit'),
                                    col("i94res").alias('i94_res'),
                                    col("i94port").alias('i94_city_cd'),
                                    col("arrdate").alias('arr_date'),
                                    col("depdate").alias('dep_date'),
                                    col("i94addr").alias('i94_state_cd'),
                                    col("i94bir").alias('i94_resp_age'),
                                    col("i94visa").alias('i94_visa_cd'),
                                    col("visapost").alias('visa_post'),
                                    col("dtaddto").alias('us_adm_date'),
                                    col("admnum").alias('adm_num'),
                                    col("gender").alias('gender'),
                                    col("insnum").alias('ins_num'),
                                    col("airline").alias('airline'),
                                    col("fltno").alias('fl_num')
                            ).dropDuplicates()
    
    # write fact_immigration table to parquet files
    fact_immigration_94_table.write.mode('overwrite').partitionBy('i94_state_cd') \
                        .parquet(os.path.join(output_data, 'fact_immigration_i94.parquet'))
    
    print("immigration data has been written")
    dataframes["fact_immigration_94_table"] = fact_immigration_94_table
    
    return dataframes
    
    
def main():
    """
    Calls method to create Spark Session.
    Sets paths for S3 buckets (outputs)
    Calls method to create fact and dimension tables
    """
    spark = create_spark_session()
    output_data = "s3a://shale-udacity/"
    dataframes = {}
    
    dataframes = process_i94_labels(spark, output_data, dataframes)    
    dataframes = process_city_demographics_data(spark, output_data, dataframes)
    dataframes = process_immigration_data(spark, output_data, dataframes)
    
    # run data quality checks
    for table_name, dataframe in dataframes.items():
        # check to see if the tables have data
        print("Starting quality checks")
        quality_checks.check_data(dataframe, table_name)
        print("Quality checks complete")
    
    
    
if __name__ == "__main__":
    main()
import pandas as pd
import os
import configparser
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import isnan, when, count, avg, col, udf, dayofmonth, dayofweek, month, year, weekofyear, monotonically_increasing_id
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('credentials.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creates Spark Session to be used for data processing.
    """
    
    # spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11").enableHiveSupport().getOrCreate()

    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

    spark = SparkSession.builder.getOrCreate()
    
    return spark

def clean_dataset(df, subset=None, drop_cols=False, thresh=0.7):
    """
    Cleans-up dataset by dropping duplicates and NaN's/Nulls.
    Optionally, it drops columns with high % of nulls.
    Input:
        df: Spark DataFrame
        subset: default None. List of column names to take into account.
        drop_cols: default False. Whether to drop columns with high % of Nulls.
        thresh: default 0.7. Threshold for deciding % of nulls of column to drop.
    """
    
    df = df.dropna(how='all', subset=subset)
    df = df.dropDuplicates(subset)
    
    if drop_cols:
        total_row_count = df.count()
        col_nulls_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
        col_nulls_df = pd.melt(col_nulls_df, var_name='Column Name', value_name='Null Count')
        col_nulls_df['Ratio to total'] = (col_nulls_df['Null Count']/total_row_count).round(3)
        cols = col_nulls_df.loc[col_nulls_df['Ratio to total'] > 0.7].columns.tolist()
        df = df.drop(*cols)
    
    return df

def create_dim_calendar_table(df, output_location):
    """
    Creates the dim_calendar table for the data model.
    Input:
        df: Spark DataFrame.
        output_location: String. Directory or S3 location to write the parquet file.
    """
    
    # udf to convert dt into a datetime data type.
    get_dt = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    # create initial dataFrame, extract required columns, date & time formats.
    calendar = df.select(['arrdate']).withColumn("arrdate", get_dt(df.arrdate)).distinct()
    calendar = calendar.withColumn("id", monotonically_increasing_id())
    calendar = calendar.withColumn("year", year("arrdate"))
    calendar = calendar.withColumn("month", month("arrdate"))
    calendar = calendar.withColumn("day", dayofmonth("arrdate"))
    calendar = calendar.withColumn("week", weekofyear("arrdate"))
    calendar = calendar.withColumn("weekday", dayofweek("arrdate"))
    
    # write to parquet file
    partition_cols = ["year", "month"]
    calendar.write.parquet(output_location + "dim_calendar", partitionBy=partition_cols, , mode="overwrite")
    
    return calendar

def create_dim_usa_demographics_table(df, output_location):
    """
    Creates the dim_usa_demographics table for the data model.
    Input:
        df: Spark DataFrame.
        output_location: String. Directory or S3 location to write the parquet file.
    """
    
    # create DataFrame with required column names
    demog_df = df.withColumnRenamed("City", "city")\
                .withColumnRenamed("State", "state") \
                .withColumnRenamed("Median Age", "median_age") \
                .withColumnRenamed("Male Population", "male_pop") \
                .withColumnRenamed("Female Population", "female_pop") \
                .withColumnRenamed("Total Population", "total_pop") \
                .withColumnRenamed("Number of Veterans", "veteran_number") \
                .withColumnRenamed("Foreign-born", "foreign_born") \
                .withColumnRenamed("Average Household Size", "avg_household_size") \
                .withColumnRenamed("Race", "race") \
                .withColumnRenamed("Count", "count")
    
    # add id column
    demog_df = df.withColumnRenamed("id", monotonically_increasing_id())
    
    # write parquet file
    demog_df.write.parquet(output_location + "dim_usa_demographics", mode="overwrite")
    
    return demog_df

def create_dim_immigrant_table(df, output_location):
    """
    Creates the dim_immigrant table for the data model.
    Input:
        df: Spark DataFrame.
        output_location: String. Directory or S3 location to write the parquet file. 
    """
    
    # create a DataFrame with required columns.
    cols = [
        "ccid"
        , "i94cit"
        , "i94res"
        , "i94mode"
        , "depdate"
        , "i94bir"
        , "gender"
    ]
    im_df = df.select(cols)
    
    # write parquet file
    im_df.write.parquet(output_location + "dim_immigrant", mode="overwrite")
    
    return im_df

def create_dim_temperatures_table(df, output_location):
    """
    Creates the dim_temperatures table for the data model.
    Input:
        df: Spark DataFrame.
        output_location: String. Directory or S3 location to write the parquet file. 
    """
    
    # create a DataFrame with required columns.
    temp_df = df.withColumnRenamed("dt", "dt") \
                .withColumnRenamed("AverageTemperature", "avg_temp") \
                .withColumnRenamed("AverageTemperatureUncertainty", "avg_temp_uncertainty") \
                .withColumnRenamed("City", "city") \
                .withColumnRenamed("Country", "country") \
                .withColumnRenamed("Latitude", "latitude") \
                .withColumnRenamed("Longitude", "longitude")
    
    temp_df.write.parquet(output_location + "dim_temperatures", mode="overwrite")
    
    return temp_df

def create_fact_i94_immigration_table(df, output_location):
    """
    Creates the fact_i94_immigration table for the data model.
    Input:
        df: Spark DataFrame.
        output_location: String. Directory or S3 location to write the parquet file. 
    """
    
    # udf to convert dt into a datetime data type.
    get_dt = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
        
    # create a DataFrame with required columns.
    cols = [
        "ccid"
        , "arrdate"
        , "count"
        , "visapost"
        , "entdepa"
        , "entdepd"
        , "biryear"
        , "dtaddto"
        , "airline"
        , "fltno"
        , "visatype"
        , "i94addr"
        , "i94visa"
    ]
    i94_df = df.select(cols)
    
    # convert arrdate column into Datatime data type
    i94_df = i94_df.withColumn("arrdate", get_dt(df.arrdate))
    
    # write parquet file
    i94_df.write.parquet(output_location + "fact_i94_immigration", mode="overwrite")
    
    return i94_df

def process_i94_data(spark, input_location, output_location, datadir):
    """
    ETL for the i94 immigration data. 
    Creates the dim_calendar, dim_immigrant and fact_i94_immigration tables.
    Input:
        spark: Spark Session.
        input_location: String. Directory or S3 location to read file.
        output_location: String. Directory or S3 location to write the parquet file. 
        datadir: String. Directory with parquet files with source data.
    """
    
    # read files into Spark DataFrame
    i94_data = spark.read.load(datadir)
    
    # clean-up i94 data
    i94_df = clean_dataset(i94_data, subset=None, drop_cols=False, thresh=0.7)
    
    # create dim_calendar table and save to parquet file in output location
    calendar_table = create_dim_calendar_table(i94_df, output_location)
    
    # create dim_immigrant table and save to parquet file in output location
    im_table = create_dim_immigrant_table(i94_df, output_location)
    
    # create fact_i94_immigration table and save to parquet file in output location
    i94_table = create_fact_i94_immigration_table(i94_df, output_location)
    
def process_usa_demog_data(spark, input_location, output_location, fname):
    """
    ETL for the USA Demographics data. 
    Creates the dim_calendar, dim_immigrant and fact_i94_immigration tables.
    Input:
        spark: Spark Session.
        input_location: String. Directory or S3 location to read file.
        output_location: String. Directory or S3 location to write the parquet file. 
        fname: String. Name of files with source data.
    """
    
    # read file into Spark DataFrame
    demog_data = spark.read.csv(fname, header=True, inferSchema=True, sep=';')
    
    # clean-up demographics data
    demog_df = clean_dataset(demog_data, subset=None, drop_cols=False, thresh=0.7)
    
    # create dim_usa_demographics table and save to parquet file in output location
    demog_table = create_dim_usa_demographics_table(demog_df, output_location)

def process_temperatures_data(spark, input_location, output_location, fname):
    """
    ETL for the Tables data. 
    Creates the dim_calendar, dim_immigrant and fact_i94_immigration tables.
    Input:
        spark: Spark Session.
        input_location: String. Directory or S3 location to read file.
        output_location: String. Directory or S3 location to write the parquet file. 
        fname: String. Name of files with source data.
    """
    
    # read file into Spark DataFrame
    temp_data = spark.read.csv(fname, header=True, inferSchema=True, sep=';')
    
    # clean-up demographics data
    temp_df = clean_dataset(temp_data, subset=None, drop_cols=False, thresh=0.7)
    
    # create dim_usa_demographics table and save to parquet file in output location
    temp_table = create_dim_temperatures_table(temp_df, output_location)

def data_quality_checks(df):
    """
    Runs data quality checks against newly created table in the data model.
    Input:
        df: Spark DataFrame.
    """
    
    total_rows = df.count()
    no_duplicate_rows = df.dropDuplicates(how='all').count()
    total_duplicates = total_rows - no_duplicate_rows
    
    if total_rows == 0:
        return "Data quality check failed. Table has 0 records."
    else:
        if total_duplicates == 0:
            return f"Data quality check failed. Table has {total_duplicates} duplicates."
        else:
            return f"Data quality check passed. Table has {total_rows} and no duplicates."

def main():
    input_location = ''
    output_location = ''
    
    spark = create_spark_session()
    
    process_i94_data(spark, input_location, output_location, datadir='./sas_data')
    process_usa_demog_data(spark, input_location, output_location, fname='../../data2/GlobalLandTemperaturesByCity.csv')
    process_temperatures_data(spark, input_location, output_location, fname='us-cities-demographics.csv')

if __name__ == "__main__":
    main()
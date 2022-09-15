import json
import os
import string
from datetime import datetime
import pyspark
from pyspark.sql.functions import lit,unix_timestamp
import pandas as pd

import time
import datetime

from google.cloud import storage
from platform import python_version
from pyspark.sql.types import *
from pyspark.sql.functions import *


timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


# data processing functions
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')\
    .getOrCreate()



def file1():
    """
    this function get the json file, read and append in parquet file in a new bucket GCS
    """
    json_file_path = 'gs://bucket-dataproc-ge/great_expectations/validations/teste_tabela_ge/__none__/20220318T172550.578808Z/b82d2b0bc0d5deaf8922db55075f898b.json'
    df2 = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").option("inferSchema", "true").json(json_file_path)
    
    gcs_bucket = 'sandbox-coe'
    path = 'gs://bucket-raw-ge/raw-ge-files/' 
    df2.write.format("parquet").option("path", "gs://bucket-raw-ge/raw-ge-files/").save(mode='append')
    

    
    
def file2():
    """
    this function gets the last parquet files in the bucket and returns a new dataframe
    
    """
    client = storage.Client()
    bucket = client.get_bucket("bucket-raw-ge")
    blobs = client.list_blobs(bucket_or_name=bucket, prefix='raw-ge-files/') 

    today = datetime.date.today()
    list1 = [] 

    for blob in blobs:
        today = datetime.date.today()
        x = blob.updated
        date = x.date()
        if date == today:
            list1 = f'gs://bucket-raw-ge/{blob.name}'
    
    path_parquet = list1
    df3 = spark.read.parquet(path_parquet)
    return df3.printSchema()


def flatten(df): 
   """
   this function will read the partquet new dataframe and desnormalize in to a new dataframe
   """ 
   complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   while len(complex_fields)!=0:
      col_name=list(complex_fields.keys())[0]
      #print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
      if (type(complex_fields[col_name]) == StructType):
         expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
         df=df.select("*", *expanded).drop(col_name)
    
      elif (type(complex_fields[col_name]) == ArrayType):    
         df=df.withColumn(col_name,explode_outer(col_name))
        
      complex_fields = dict([(field.name, field.dataType)
                             for field in df.schema.fields
                             if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
   return df

df = flatten(file2)


def df_cleaner2():
    """
    This function will rename the coluns new datatypes and drop columns we dont use.
    """
    df_cleaner2 = (df.withColumn("expectations_sucesso", col("success").cast("string"))
              .withColumn("resultado_sucesso", col("results_success").cast("string"))
              .withColumn("qtd_inesperado", col("results_result_unexpected_count").cast("int"))
              .withColumnRenamed("meta_expectation_suite_name","nm_suite")
              .withColumnRenamed("meta_active_batch_definition_data_asset_name", "nm_tabela")
              .withColumnRenamed("meta_active_batch_definition_data_connector_name", "nm_conexao_batch")
              .withColumnRenamed("meta_active_batch_definition_datasource_name","nm_source_execucao")
              .withColumnRenamed("meta_run_id_run_name","nm_execucao_expectation")
              .withColumn("timestamp_execucao_expectation", unix_timestamp(col("meta_run_id_run_time"), 'yyyy-MM-dd HH:mm:ss').cast("timestamp")) # esse campo ai precisa rever pq só está trazendo nulo
              .withColumnRenamed("results_exception_info_exception_message","resultado_info_excecao")
              .withColumnRenamed("results_exception_info_exception_traceback","resultado_info_traceback")
              .withColumn("resultado_info_raised", col("results_exception_info_raised_exception").cast("string"))
              .withColumnRenamed("results_expectation_config_expectation_type","ft_nm_regra_expectation")
              .withColumn("qtd_elemento", col("results_result_element_count").cast("int"))
              .withColumn("qtd_elemento_faltante", col("results_result_missing_count").cast("int"))
              .withColumn("porcentagem_faltante", col("results_result_missing_percent").cast("string"))
              .withColumn("porcentagem_nao_faltante", col("results_result_unexpected_percent_nonmissing").cast("string"))
              .withColumnRenamed("results_result_unexpected_percent","porcentagem_inesperada")
              .withColumnRenamed("results_expectation_config_kwargs_batch_id", "id_batch_coluna")
              .withColumnRenamed("results_expectation_config_kwargs_column", "nm_coluna")
              .withColumnRenamed("results_result_observed_value", "resultado_geral")
              .withColumn('dt_insert_fato',unix_timestamp(lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    
              .drop("meta_great_expectations_version" , "meta_batch_markers_ge_load_time", "meta_run_id_run_time",
                    "meta_batch_spec_batch_data", "meta_batch_spec_data_asset_name",
                    "meta_active_batch_definition_batch_identifiers_batch_id", "results_result_element_count",
                    "results_result_missing_count", "results_expectation_config_expectation_context_description",
                    "results_result_partial_unexpected_counts_count", "success", "results_exception_info_raised_exception",
                    "results_success", "results_result_missing_percent", "results_result_unexpected_percent_nonmissing", "results_result_unexpected_percent_total", 
                    "results_expectation_config_kwargs_mostly", "meta_validation_time", "statistics_evaluated_expectations",
                    "statistics_success_percent", "statistics_successful_expectations", "statistics_unsuccessful_expectations", 
                    "results_result_observed_value", "results_expectation_config_kwargs_column_list", "results_expectation_config_kwargs_value", 
                    "results_expectation_config_kwargs_value_set", "results_result_partial_unexpected_index_list", "results_result_partial_unexpected_list", "results_result_partial_unexpected_counts_value",
                   "results_result_unexpected_count", "results_expectation_config_kwargs_parse_strings_as_datetimes",
                   "results_expectation_config_kwargs_strictly", "results_result_details_mismatched", "results_expectation_config_kwargs_column_set",
                   "results_result_details_value_counts_count","results_result_details_value_counts_value"))
    return df_cleaner2.printSchema()



def df_dim_dimensoes():

    """
    this function reads the table dim_dimensioes in big query 
    """
    table = "sandbox-coe.trusted_ge_dataquality.dim_dimensoes"

    df_dim_dimensoes = spark.read \
      .format("bigquery") \
      .option("table", table) \
      .load()
    return df_dim_dimensoes.printSchema()


def df_dim_regras_ge():

    """
    this function reads the table dim_regras_expectations in big query 
    """
    table = "sandbox-coe.trusted_ge_dataquality.dim_regras_expectations"
    
    df_dim_regras_ge = spark.read \
      .format("bigquery") \
      .option("table", table) \
      .load()
    return df_dim_regras_ge.printSchema()
    


def df_final():
    """
    this function will join the 2 dimesion with the new fact and returns the final dataframe
    """
    df_final = (df_cleaner2.join(df_dim_regras_ge, df_cleaner2.ft_nm_regra_expectation == df_dim_regras_ge.nm_regra_expectation,how='left')
             .drop('tipo_regra_expectation', 'desc_regra_expectation', 'dt_insert', 'nm_regra_expectation')
             .withColumn("id_regra_expectation", col("id_regra_expectation").cast("int"))
             .withColumn("id_dimensao", col("id_dimensao").cast("int")))

    return df_final.printSchema()



def dataquality():

    """
    This function will check for data quality
    """
    
    check_columns = df_dim_dimensoes.columns
    if check_columns == df_dim_dimensoes.columns:
        print("Number of Columns Test Passed!")
    else:
        raise ValueError("Number of Columns does not Match with Expected Value!")
        
    if df_dim_dimensoes.count() > 0:
        print("this table is not null Test Passed!")
    else:
        raise ValueError("This table have null values, does not Match with Expected Value!")
    
    
    check_columns2 = df_dim_regras_ge.columns
    if check_columns2 == df_dim_regras_ge.columns:
        print("Number of Columns Test Passed!")
    else:
        raise ValueError("Number of Columns does not Match with Expected Value!")


    if df_dim_regras_ge.count() > 0:
        print("this table is not null Test Passed!")
    else:
        raise ValueError("This table have null values, does not Match with Expected Value!")


    check_columns3 = df_final.columns
    if check_columns3 == df_final.columns:
        print("Number of Columns Test Passed!")
    else:
        raise ValueError("Number of Columns does not Match with Expected Value!")

    if df_final.count() > 0:
        print("this table is not null Test Passed!")
    else:
        raise ValueError("This table have null values, does not Match with Expected Value!")



def insert_bq():

    """
    this function will write the new table fact on big query based on final dataframe.
    """
    gcs_bucket = 'bucket-raw-ge/temp-files'
    bq_dataset = 'trusted_ge_dataquality'
    bq_table = 'fato_ge_dataquality' 

    df_final.write \
      .format("bigquery") \
      .option("table","{}.{}".format(bq_dataset, bq_table)) \
      .option("temporaryGcsBucket", gcs_bucket) \
      .mode('append') \
      .save()
    
    
spark.stop()





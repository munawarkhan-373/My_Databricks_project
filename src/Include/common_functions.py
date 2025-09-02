# Databricks notebook source
# MAGIC %run "../Include/configurations"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    outout_df = input_df.withColumn('Ingestion_date', current_timestamp())
    return outout_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []

  for column_names in input_df.schema.names:
    if column_names != partition_column:
      column_list.append(column_names)

  column_list.append(partition_column)

  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")
        
    
 
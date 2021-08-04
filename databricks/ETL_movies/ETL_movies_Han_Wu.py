# Databricks notebook source
# MAGIC %md
# MAGIC ## Define Variables
# MAGIC 
# MAGIC Assign path to DBFS, we will update 5 silver tables for movie casting activity.

# COMMAND ----------

bronzetable = "movie_classic_bronze"
silver_tablename = ['clean_genres_silver','clean_movie_silver','clean_OriginalLanguage_silver','clean_revenue_silver','clean_movie_genres_silver']

user = 'Han_Wu'
classicPipelinePath = f"/dbassignment1_v2/movie/user/"
bronzePath = classicPipelinePath + "bronze/"
silverPath1 = classicPipelinePath + "silver1/"
silverPath2 = classicPipelinePath + "silver2/"
silverPath3 = classicPipelinePath + "silver3/"
silverPath4 = classicPipelinePath + "silver4/"
silverPath5 = classicPipelinePath + "silver5/"
path_list = [silverPath1,silverPath2,silverPath3,silverPath4,silverPath5]

#silverQuarantinePath = classicPipelinePath + "silverQuarantine/"
#goldPath = classicPipelinePath + "gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Operation Functions

# COMMAND ----------

# MAGIC %run /Users/goodareny@gmail.com/movie_function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Datasource
# MAGIC using defined function `read_batch_raw`

# COMMAND ----------

rawpath = 'dbfs:/FileStore/shared_uploads/goodareny@gmail.com'
rawDF = read_batch_raw(rawpath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Raw

# COMMAND ----------

# display(rawDF)
rawDF.count() # got 9992 distinct orginal raw records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata
# MAGIC using defined function `transform_raw` to add metadata: `datasource`, `ingesttime`, `status`, `p_ingestdate`. The `status` is `new` once imported.

# COMMAND ----------

transformedRawDF = transform_raw(rawDF) # raw dataframe with metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze
# MAGIC use defined function `batch_writer` to write delta files into `bronzePath` 

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True) # empty bronzepath to avoid duplicated ingestion for the begining

# COMMAND ----------

rawToBronzeWriter = batch_writer( transformedRawDF, partition_column="p_ingestdate") # mode is append
rawToBronzeWriter.save(bronzePath) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore
# MAGIC use `create_table` function to create bronze table from `bronzePath`

# COMMAND ----------

create_table(spark,bronzetable, bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purge Raw File Path
# MAGIC manually delete updated raw

# COMMAND ----------

# dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(silverPath1, recurse=True)
dbutils.fs.rm(silverPath2, recurse=True)
dbutils.fs.rm(silverPath3, recurse=True)
dbutils.fs.rm(silverPath4, recurse=True)
dbutils.fs.rm(silverPath5, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records
# MAGIC `read_batch_bronze` only loaded `new` from Bronze by default

# COMMAND ----------

bronzeDF = read_batch_bronze(bronzetable)
bronzeDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Bronze
# MAGIC use `bronze_silver_tracker` function, which splits Bronze into `silver_tracker_clean`, `silver_tracker_quarantine`

# COMMAND ----------

silver_tracker_clean, silver_tracker_quarantine = bronze_silver_tracker(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Clean Batch to a Silver Table
# MAGIC * `df_clean_to_silver` gives 5 silver dataframes: `genres`, `OriginalLanguage`,`movies`, `revenue`, `movie_genres`
# MAGIC * Then use ` silver_batch_writer` writes our silver delta files into different `silverPath`, default mode is `append`

# COMMAND ----------

silver_tracker_clean_genres, silver_tracker_clean_OriginalLanguage, silver_tracker_clean_movie, silver_tracker_clean_revenue, silver_tracker_clean_movie_genres = df_clean_to_silver(cleanrecords = silver_tracker_clean)

# COMMAND ----------

df_clean_list = [silver_tracker_clean_genres,silver_tracker_clean_movie,silver_tracker_clean_OriginalLanguage,silver_tracker_clean_revenue,silver_tracker_clean_movie_genres]

for i in range(0,5):
    silver_batch_writer(df_clean_list[i]).save(path_list[i])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Silver Tables in the Metastore
# MAGIC `create_table` for silver tables

# COMMAND ----------

for i in range(0,5):
  create_table(spark, silver_tablename[i], path_list[i])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads
# MAGIC `update_status` changes status to `loaded` for silver clean data and corresponding bronze data

# COMMAND ----------

update_status(spark, silver_tracker_clean, status="loaded", path=bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Quarantined records
# MAGIC `update_status` changes status to `quarantined` for silver quarantined data and corresponding bronze data

# COMMAND ----------

update_status(spark, silver_tracker_quarantine, status="quarantined", path=bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records
# MAGIC So far, we got some quarantined records

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT status, count(*) FROM movie_classic_bronze group by status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantined Bronze Transformation
# MAGIC * `read_batch_bronze` selects quarantined data from bronze table and returns a dataframe to be cleaned
# MAGIC * then use `bronze_QuarantinedDF_transform` to clean data as required on `runtime`, `budget`
# MAGIC * use `bronze_QuarantinedDF_toSilver` to do transformation, resulting `5` silver dataframes, which uniquely combines both cleaned data from quarantined and data from clean silver

# COMMAND ----------

bronze_QuarantinedDF = read_batch_bronze(bronzetable, status = 'quarantined')
bronzeQuarantinedDF_tracker_trans = bronze_QuarantinedDF_transform(bronze_QuarantinedDF) # transfor quarantined data as requires

# COMMAND ----------

silver_bronzeQuarantinedDF_genres,silver_bronzeQuarantinedDF_OriginalLanguage,silver_bronzeQuarantinedDF_movie,silver_bronzeQuarantinedDF_revenue,silver_bronzeQuarantinedDF_movie_genre = bronze_QuarantinedDF_toSilver(bronzeQuarantinedDF_tracker_trans)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Clean Quarantined Batch to Silver
# MAGIC ` silver_batch_writer`overwirte silver tables, changing mode to `overwrite`

# COMMAND ----------

df_quarantined_list = [silver_bronzeQuarantinedDF_genres,silver_bronzeQuarantinedDF_movie,silver_bronzeQuarantinedDF_OriginalLanguage,silver_bronzeQuarantinedDF_revenue,silver_bronzeQuarantinedDF_movie_genre]
path_list = [silverPath1,silverPath2,silverPath3,silverPath4,silverPath5]

for i in range(0,5):
    silver_batch_writer(df_quarantined_list[i], mode = 'overwrite').save(path_list[i])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Silver Tables from Clean Clean Quarantined Batch
# MAGIC `create_table` to update silver tables

# COMMAND ----------

for i in range(0,5):
  create_table(spark, silver_tablename[i], path_list[i])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Quarantined records

# COMMAND ----------

update_status(spark, bronzeQuarantinedDF_tracker_trans, status="loaded", path=bronzePath)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT status, count(*) FROM movie_classic_bronze group by status

# Databricks notebook source
from pyspark.sql.functions import explode
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.functions import monotonically_increasing_id 
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

def read_batch_raw(rawpath: str):
    raw_tracker_data_df = spark.read.option("inferSchema", "true").option("multiline","true").json(rawpath)
    raw_tracker_data_df = raw_tracker_data_df.select(explode(raw_tracker_data_df.movie).alias('value')).distinct()
    return raw_tracker_data_df

# COMMAND ----------

def transform_raw(raw):
    return raw.select(
                           'value',
                           lit("ADB Movie Shop").alias("datasource"),
                           current_timestamp().alias("ingesttime"),
                           lit("new").alias("status"),
                           current_timestamp().cast("date").alias("p_ingestdate")
    )
  

# COMMAND ----------

def batch_writer(
    dataframe,
    partition_column: str,
    exclude_columns: list = [],
    mode: str = "append",
):
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )


# COMMAND ----------

def read_batch_bronze(tablename:str, status = 'new'):
    bronzeDF = spark.table(tablename).filter(col('status') == status)
    return bronzeDF

# COMMAND ----------

def bronze_silver_tracker(bronzedf):
  
    silver_tracker = bronzedf.select('value',col('ingesttime').alias('eventdate'),"value.*")
    silver_tracker_quarantine = silver_tracker.where((col('RunTime')<0) | (col('Budget')<1000000) )
    silver_tracker_clean = silver_tracker.where( (col('RunTime')>=0) | (col('Budget')>=1000000))
    
    return silver_tracker_clean, silver_tracker_quarantine

# COMMAND ----------

def df_clean_to_silver(cleanrecords):
  
    silver_tracker_clean_genres = silver_tracker_clean.select(explode('genres')).distinct().select('col.*').where(col('name')!="")\
                                 .withColumnRenamed('id','genres_id').withColumnRenamed('name','genres_name')
   
    silver_tracker_clean_OriginalLanguage= silver_tracker_clean.select('OriginalLanguage').distinct().withColumn("language_id", monotonically_increasing_id())
    
    silver_tracker_clean_OriginalLanguage.createOrReplaceTempView('clean_records_languages_tempview')
    silver_tracker_clean.select('eventdate','BackdropUrl','CreatedBy','CreatedDate','Id','ImdbUrl','OriginalLanguage','PosterUrl','Price','ReleaseDate','Revenue',\
                                'RunTime','TmdbUrl','UpdatedBy','UpdatedDate','Budget','Title','Overview','Tagline').createOrReplaceTempView('clean_records_tempview')
    silver_tracker_clean_records = spark.sql('select c1.*, c2.language_id from clean_records_tempview c1 join clean_records_languages_tempview c2 on c1.OriginalLanguage = c2.OriginalLanguage').drop('OriginalLanguage')
    silver_tracker_clean_movie = silver_tracker_clean_records.select('id','language_id', 'Budget', 'Title','Overview','Tagline','RunTime')
    
    silver_tracker_clean_revenue = silver_tracker_clean_records.select( 'eventdate', 'Id','Price','Revenue','ReleaseDate','BackdropUrl','CreatedBy','CreatedDate',\
                            'ImdbUrl','PosterUrl','TmdbUrl','UpdatedBy', 'UpdatedDate')

    silver_tracker_clean_movie_genres = silver_tracker_clean.select('id',explode('genres.id').alias('genres_id'))
    
    return silver_tracker_clean_genres, silver_tracker_clean_OriginalLanguage, silver_tracker_clean_movie, silver_tracker_clean_revenue, silver_tracker_clean_movie_genres

# COMMAND ----------

def silver_batch_writer(dataframe,mode="append"):
    return (dataframe.write.format("delta").mode(mode) )


# COMMAND ----------

def bronze_QuarantinedDF_transform(df):
    return df.select('value',col('ingesttime').alias('eventdate'),"value.*")\
    .withColumn('RunTime',-1*col('RunTime')).withColumn('Budget', lit(100000).cast('Long'))

# COMMAND ----------

def bronze_QuarantinedDF_toSilver(bronzeQuarantined_trans):

    silver_bronzeQuarantinedDF_genres = bronzeQuarantined_trans.select(explode('genres')).distinct().select('col.*')\
                                                              .where(col('name')!="").withColumnRenamed('id','genres_id')\
                                                              .withColumnRenamed('name','genres_name')\
                                                              .union(spark.table('clean_genres_silver'))
    
    bronzeQuarantinedDF_trans_OriginalLanguage= bronzeQuarantined_trans.select('OriginalLanguage').distinct()
    silver_bronzeQuarantinedDF_OriginalLanguage = silver_tracker_clean_OriginalLanguage\
                                                        .join(bronzeQuarantinedDF_trans_OriginalLanguage,\
                                                              silver_tracker_clean_OriginalLanguage.OriginalLanguage==bronzeQuarantinedDF_trans_OriginalLanguage.OriginalLanguage
                                                              ,'outer')\
                                                        .withColumn('language_id',monotonically_increasing_id())\
                                                        .drop(bronzeQuarantinedDF_trans_OriginalLanguage.OriginalLanguage)
 
    bronzeQuarantined_trans.select('eventdate','BackdropUrl','CreatedBy','CreatedDate','Id','ImdbUrl','OriginalLanguage','PosterUrl','Price',\
                                   'ReleaseDate','Revenue','RunTime','TmdbUrl','UpdatedBy','UpdatedDate','Budget','Title','Overview','Tagline')\
                           .createOrReplaceTempView('temp1') 
    silver_bronzeQuarantinedDF_OriginalLanguage.createOrReplaceTempView('temp2')
    silver_bronzeQuarantinedDF_records = spark.sql('select temp1.*, temp2.language_id from temp1 join temp2 on temp1.OriginalLanguage = temp2.OriginalLanguage').drop('OriginalLanguage')
        
    silver_bronzeQuarantinedDF_movie = silver_bronzeQuarantinedDF_records.select('id','language_id', 'Budget','Title','Overview','Tagline','RunTime').union(spark.table('clean_movie_silver'))

    silver_bronzeQuarantinedDF_revenue =silver_bronzeQuarantinedDF_records.select('eventdate','Id','Price','Revenue','ReleaseDate','BackdropUrl', 'CreatedBy','CreatedDate',\
                                                                                           'ImdbUrl','PosterUrl','TmdbUrl','UpdatedBy','UpdatedDate')\
                                                                                    .union(spark.table('clean_revenue_silver'))   

    silver_bronzeQuarantinedDF_movie_genres = bronzeQuarantined_trans.select('id',explode('genres.id').alias('genres_id')).union(spark.table('clean_movie_genres_silver'))
    
    return silver_bronzeQuarantinedDF_genres, silver_bronzeQuarantinedDF_OriginalLanguage,silver_bronzeQuarantinedDF_movie,silver_bronzeQuarantinedDF_revenue,silver_bronzeQuarantinedDF_movie_genres

# COMMAND ----------

def create_table(spark,tablename,path):
  spark.sql(
  f"""
  DROP TABLE IF EXISTS {tablename}
  """
  )

  spark.sql(
  f"""
  CREATE TABLE {tablename}
  USING DELTA
  LOCATION "{path}"
  """
  )

# COMMAND ----------


def update_status(spark, df, status:str, path):
  
  bronzeTable = DeltaTable.forPath(spark, path)
  silverAugmented = df.withColumn("status", lit(status))
  update_match = "bronze.value = clean.value"
  update = {"status": "clean.status"}

  (
  bronzeTable.alias("bronze")
  .merge(silverAugmented.alias("clean"), update_match)
  .whenMatchedUpdate(set=update)
  .execute()
  )


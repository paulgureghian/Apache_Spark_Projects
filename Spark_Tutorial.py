# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import window,col,desc

# COMMAND ----------

eventsDF = spark.read.json("/FileStore/tables/videodata.json") 

# COMMAND ----------

display(eventsDF)  

# COMMAND ----------

jsonSchema = StructType([StructField("customer_id", LongType(), True),StructField("show_id", LongType(), True), StructField("state", StringType(), True), StructField("timestamp", TimestampType(), True) ])

# COMMAND ----------

eventsTypedDF = spark.read.schema(jsonSchema).json("/FileStore/tables/videodata.json") 

# COMMAND ----------

eventsTypedDF.head() 

# COMMAND ----------

display(eventsTypedDF) 

# COMMAND ----------

eventsTypedDF.createOrReplaceTempView("video_events" )

# COMMAND ----------

eventsTypedDF.count() 

# COMMAND ----------

eventsTypedDF. where(eventsTypedDF.state == "open").count() 

# COMMAND ----------

spark.sql("SELECT count(*) FROM video_events WHERE state = 'open'").show() 

# COMMAND ----------

spark.sql("SELECT show_id, count(*) as open_events from video_events WHERE state == 'open' group by show_id ORDER BY  open_events DESC").show() 

# COMMAND ----------

videoCounts = eventsTypedDF.groupby(eventsTypedDF.state, window(eventsTypedDF.timestamp, "5 minutes")).count() 
videoCounts.createOrReplaceTempView("static_counts") 

# COMMAND ----------

windowDF = spark.sql("SELECT  show_id, state, window(timestamp, '5 minutes') as window,count(*) as count from video_events group by state,show_id, window order by window, count,show_id DESC") 

# COMMAND ----------

windowDF.write.json("/FileStore/tables/window1.json") 

# COMMAND ----------

# MAGIC %sql select state, sum(count) as total_count from static_counts group by state 

# COMMAND ----------

spark.sql('select state, date_format(window.end, "YYYY-MM-dd HH:mm") as time, count from static_counts order by time, state').show() 

# COMMAND ----------

streamingInputPath = "/FileStore/tables/video*.json"
eventsStreamingDF = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(streamingInputPath) 

# COMMAND ----------

eventsStreamingDF.isStreaming 

# COMMAND ----------

query = eventsStreamingDF.writeStream.format("memory").queryName("streaming_events").start() 

# COMMAND ----------

# MAGIC %sql select count(*) from streaming_events where state = 'open' 

# COMMAND ----------

eventsStreamingDF.stop() 

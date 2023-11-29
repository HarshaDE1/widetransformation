# Databricks notebook source
df=spark.read.format('csv').option("header", "true").load('dbfs:/FileStore/students__data.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

#import libraries which is required for defining schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

#define schema using libraries
schema=StructType([StructField("roll_no",IntegerType(),True),StructField("Student_name",StringType(),True),StructField("Marks",IntegerType(),True)])

# COMMAND ----------

#3 types of method used for handling badrecords

# COMMAND ----------

# DBTITLE 1,PERMISSIVE
#it include corrupt record and display table with
Df=spark.read.format('csv').option("header", "true").option("mode","PERMISSIVE").schema(schema).load('dbfs:/FileStore/students__data.csv')
display(Df)

# COMMAND ----------

# DBTITLE 1,DropMalformed
#Drop Malformed- Ignore corrupt records and display data
Data=spark.read.format('csv').option("header", "true").option("mode","DropMalformed").schema(schema).load('dbfs:/FileStore/students__data.csv')
display(Data)

# COMMAND ----------

# DBTITLE 1,FailFast
#it will through exception
Df2=spark.read.format('csv').option("header", "true").option("mode","FailFast").schema(schema).load('dbfs:/FileStore/students__data.csv')
display(Df2)

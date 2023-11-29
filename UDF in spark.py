# Databricks notebook source
#PySpark UDF is a User Defined Function that is used to create a reusable function in Spark.
#Once UDF created, that can be re-used on multiple DataFrames and SQL (after registering).
#The default type of the udf() is StringType.
#You need to handle nulls explicitly otherwise you will see side-effects.

# COMMAND ----------

# DBTITLE 1,UDF for printing a massage
#create a dataframe using csv file from DBFS and import a functions which is required for creating UDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, udf
# Create a spark session using getOrCreate() function
spark = SparkSession.builder.getOrCreate()
# reading file in spark using dataframe
df=spark.read.csv("dbfs:/FileStore/students.csv",header=True)
df.show()

# COMMAND ----------

#create a python function using define function
def display_result(Student_name,Marks):
    Result=f"""Hello {Student_name} ,Your marks is {Marks} congratulations."""
    return Result

# COMMAND ----------

#calling a fuction and check return result
display_result("Rita",200)

# COMMAND ----------

#convert python function in Spark UDF
udf_display_result=f.udf(display_result)

# COMMAND ----------

#Create a new column by calling the user defined function created
df.withColumn('message',udf_display_result(f.col('Student_name'),f.col('Marks'))).show(truncate=False)

# COMMAND ----------

#using different code for Creating a new column by calling the user defined function created
df.select("Student_name","Marks", udf_display_result("Student_name","Marks")).show(truncate=False)

# COMMAND ----------

#using different method for ceating UDF
udf_display_result1=udf(lambda z:display_result(z))

# COMMAND ----------

udf_display_result1("rita","200")

# COMMAND ----------

#Using UDF on SQL
spark.udf.register("udf_display_result1", display_result)
df.createOrReplaceTempView("DATAnew_TABLE")
spark.sql("select roll_no,Student_name,Marks, udf_display_result1(Student_name,Marks) as Msg from DATAnew_TABLE") \
     .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,UDF for converting lower case to upper case

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

columns = ["Sno","Name"]
data = [("1", "jolly jones"),
    ("2", "tonny smith"),
    ("3", "annie xanders")]

df1 = spark.createDataFrame(data=data,schema=columns)

df1.show(truncate=False)

# COMMAND ----------

#creating a python function
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

# COMMAND ----------


from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Converting function to UDF 
convertUDF = udf(lambda z: convertCase(z))

# COMMAND ----------

#creating a new col using UDF
df1.select(col("Sno"),convertUDF(col("Name")).alias("New Name") ).show(truncate=False)

# COMMAND ----------

#creating a python function for converting name in upper case
def upperCase(str):
    return str.upper()

# COMMAND ----------

#creating UDF using python fucn and printing data by creating new column
upperCaseUDF = udf(lambda z:upperCase(z),StringType())
df1.withColumn("Created Name", upperCaseUDF(col("Name"))).show(truncate=False)

# COMMAND ----------

#Using UDF on SQL for printing first letter in upper case
spark.udf.register("convertUDF", convertCase,StringType())
df1.createOrReplaceTempView("DATA_TABLE")
spark.sql("select Sno, Name,convertUDF(Name) as changed_Name from DATA_TABLE") \
     .show(truncate=False)

# COMMAND ----------

#using SQL PRINTING name in Upper case
spark.udf.register("upperCaseUDF", upperCase,StringType())
df1.createOrReplaceTempView("New_TABLE")
spark.sql("select Sno, upperCaseUDF(Name) as changed_Name from DATA_TABLE") \
     .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,UDF for creating new column and printing Square root of number
#creating a DF using pyspark library
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,FloatType
from pyspark.sql.functions import udf
  
spark = SparkSession.builder.getOrCreate()
  
cms = ["Name","RawScore"]
data =  [("Jack", 64),("Mira", 81),("Carter", 90)]                    
df3 = spark.createDataFrame(data=data,schema=cms) 
df3.show()

# COMMAND ----------

#creating a python function by importing math library 
import math
def SQRT(x):
    score=float(math.sqrt(x)+3)
    return round(score,2)

# COMMAND ----------

#converting in UDF
UDF_marks1 = udf(lambda m: SQRT(m))

# COMMAND ----------

#creating a new column for printing square root of the value
df3.select("Name","RawScore", UDF_marks1("RawScore")).show()

# COMMAND ----------

#Using Sql
spark.udf.register("UDF_marks1", SQRT,FloatType())
df3.createOrReplaceTempView("newDATA_TABLE")
spark.sql("select Name,RawScore, UDF_marks1(RawScore) as changed_score from newDATA_TABLE") \
     .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,program to sort list using UDF in PySpark
# Import the SparkSession, Row, UDF, ArrayType, IntegerType
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, IntegerType
  
# Create a spark session using getOrCreate() function
spark_session = SparkSession.builder.getOrCreate()
  
# Create a spark context
sc = spark_session.sparkContext
  
# Create a data frame with
# columns 'name' and 'marks'
df4= sc.parallelize([Row(name='Ratna', 
                         marks=[99, 68, 53]), Row(
    name='Ashita', marks=[87, 62, 59]),
                     Row(name='Vinay',
                         marks=[42, 76, 88])]).toDF().show()

# COMMAND ----------

# Create a user defined function
# to sort the ArrayType column
udf_sort = udf(lambda x: sorted(x, reverse=True),
               ArrayType(IntegerType()))

# COMMAND ----------

# Create a new column by calling the user defined function created
df4.withColumn('Sorted_Marks', 
              udf_sort(df4["marks"])).show(truncate=True)

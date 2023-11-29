# Databricks notebook source
# DBTITLE 1,Creating Dataframe
# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builder.getOrCreate()

# list of employee data
data = [["1", "sarswat", "TCST"],
		["2", "riya", "TCST"],
		["3", "rohit", "CRETA"],
		["4", "teena", "TCST"],
		["5", "vicky", "TCST"]]

# specify column names
columns = ['ID', 'NAME', 'Company']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)

dataframe.show()

# COMMAND ----------

# importing module
import pyspark

# importing sparksession from pyspark.sql module
from pyspark.sql import SparkSession

# creating sparksession and giving an app name
spark = SparkSession.builde.getOrCreate()

# list of employee data
data1 = [["1", "65000", "IT"],
		["2", "135000", "Manager"],
		["6", "50000", "HR"],
		["5", "40000", "Sales"]]

# specify column names
columns = ['ID', 'salary', 'department']

# creating a dataframe from the lists of data
dataframe1 = spark.createDataFrame(data1, columns)

dataframe1.show()

# COMMAND ----------

# DBTITLE 1,Inner join
#This will join the two PySpark dataframes on key columns, which are common in both dataframes.
# inner join on two dataframes
dataframe.join(dataframe1,
			dataframe.ID == dataframe1.ID,
			"inner").show()


# COMMAND ----------

# DBTITLE 1,Full outer join
#This will join the two PySpark dataframes on key columns, which are available in both dataframes.
dataframe.join(dataframe1, 
               dataframe.ID == dataframe1.ID, 
               "outer").show()

# COMMAND ----------

# full outer join on two dataframes
dataframe.join(dataframe1,
               dataframe.ID == dataframe1.ID, 
               "full").show()

# COMMAND ----------

# full outer join on two dataframes
dataframe.join(dataframe1, 
               dataframe.ID == dataframe1.ID,
               "fullouter").show()

# COMMAND ----------

# DBTITLE 1,Left join/leftouter join
#This will join the two PySpark dataframes on key columns, show all data which are present in left dataframe.
dataframe.join(dataframe1,
               dataframe.ID == dataframe1.ID, 
               "left").show()

# COMMAND ----------

dataframe.join(dataframe1, 
               dataframe.ID == dataframe1.ID, 
               "leftouter").show()

# COMMAND ----------

# DBTITLE 1,Right/rightouter join
#This will join the two PySpark dataframes on key columns, show all data which are present in right dataframe.
dataframe.join(dataframe1, 
               dataframe.ID == dataframe1.ID, 
               "right").show()

# COMMAND ----------

dataframe.join(dataframe1,
               dataframe.ID == dataframe1.ID, 
               "rightouter").show()

# COMMAND ----------

# DBTITLE 1,leftsemi join
#leftsemi join is similar to inner join difference being leftsemi join returns all common columns from the left dataframe and ignores all columns from the right dataframe
dataframe.join(dataframe1,
               dataframe.ID == dataframe1.ID, 
               "leftsemi").show()

# COMMAND ----------

# DBTITLE 1,left anti join
#leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records with right dataframe
dataframe.join(dataframe1,
               dataframe.ID == dataframe1.ID, 
               "leftanti").show()

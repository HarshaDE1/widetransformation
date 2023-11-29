# Databricks notebook source
# DBTITLE 1,Map Function
#Map passes each element of the source through a function and forms a new distributed dataset
l1=[8,4,2,1,15,6,9]
print(l1)

# COMMAND ----------

l2=map(lambda x:x*x,l1)

# COMMAND ----------

print(list(l2))

# COMMAND ----------

# DBTITLE 1,map function in RDD
data = sc.parallelize([10,20,30])
data.collect ()

# COMMAND ----------

data = sc.parallelize(List(10,20,30)) 

# COMMAND ----------

mapfunc = data.map(lambda x:(x,x+10))

# COMMAND ----------

mapfunc.collect ()

# COMMAND ----------

def double_even(num):
    if num % 2 == 0:
        return num * 2
    else:
        return num
 
# Create a list of numbers to apply the function to
numbers = [1, 2, 3, 4, 5]
 
# Use map to apply the function to each element in the list
result = list(map(double_even, numbers))
print(result)

# COMMAND ----------

# DBTITLE 1,Flat map
#Flat map func should return a Seq rather than a single item
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)

for element in rdd.collect():
    print(element)



# COMMAND ----------

#Flatmap    
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

# COMMAND ----------

# DBTITLE 1,reduce
#reduce is not supported in RDD
import functools
l1=[5,7,6,4,3,9]
print(functools.reduce(lambda x,y:x+y,l1))

# COMMAND ----------

lines = sc.parallelize([5,7,1,3,6,9])
lineLengths = lines.map(lambda s: len(s))
totalLength = lines.reduce(lambda a, b: a + b)

# COMMAND ----------

# DBTITLE 1,filter
#Return a new dataset formed by selecting those elements of the source on which func returns true.
l4=filter(lambda x:x>=4, l1)
print(list(l4))

# COMMAND ----------

seq = [0, 1, 2, 3, 5, 8, 13]
 
# result contains odd numbers of the list
result = filter(lambda x: x % 2 != 0, seq)
print(list(result))

# COMMAND ----------

x_rdd=sc.parallelize([1,3,5,6,9,2])
y_filter=x_rdd.filter(lambda x:(x,x%2== 0))
print('values x_rdd:(0)'.format (x_rdd.collect()))
print('values y_filter:(0)'.format (y_filter.collect()))

# COMMAND ----------

rdd3=sc.parallelize(["aman","kapil","pooja","ashish"])
rdd4 = rdd3.filter(lambda a: a.startsWith("a"))

# COMMAND ----------

# DBTITLE 1,union
#Return a new dataset that contains the union of the elements in the source dataset and the argument.
set1 = {1, 2, 3, 4, 5}
set2 = {4, 5, 6, 7, 8}
set3 = set1.union(set2)
print(set3)

# COMMAND ----------

# DBTITLE 1,Union in RDD
rdd = sc.parallelize([1, 1, 2, 3])
rdd.union(rdd).collect()

# COMMAND ----------

u1 = sc.parallelize(["c","c","p","m","t"])
u2 = sc.parallelize(["c","m","k"])
result = u1.union(u2)
result.collect()

# COMMAND ----------

# DBTITLE 1,intersection
#intersection(anotherrdd) returns the elements which are present in both the RDDs.
set1 = {1, 2, 3, 4, 5}
set2 = {4, 5, 6, 7, 8}
set3 = set1.intersection(set2)
print(set3)

# COMMAND ----------

# DBTITLE 1,Intersection in RDD
rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
rdd1.intersection(rdd2).collect()

# COMMAND ----------

# DBTITLE 1,Distinct
 #Return a new dataset that contains the distinct elements of the source dataset
 l = ['a', 'a', 'bb', 'b', 'c', 'c', '10', '10', '8','8', 10, 10, 6, 10, 11.2, 11.2, 11, 11]
distinct_l = set(l)
print(distinct_l)

# COMMAND ----------

rdd = spark.sparkContext.parallelize([1, 2, 3, 1, 2, 4])

#apply the distinct operation
distinct_rdd = rdd.distinct()

#Print the distinct elements
print(distinct_rdd.collect())

# COMMAND ----------

# DBTITLE 1,Substract
#It returns an RDD that has only value present in the first RDD and not in second RDD.
s1 = sc.parallelize(["c","c","p","m","t"])
s2 = sc.parallelize(["c","m","k"])
result = s1.subtract(s2)
print(result.collect())

# COMMAND ----------

# DBTITLE 1,Group by key
#ggregate the values of each key, using given combine functions and a neutral “zero value
from itertools import groupby

data = [('key1', 'value1'), ('key2', 'value2'), ('key1', 'value3'),('key2', 'value4')]
groups = {}
for key, group in groupby(data, lambda x: (x[0],x[1])):
    groups[key] = list(group)

print(groups)

# COMMAND ----------

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
sorted(rdd.groupByKey().mapValues(len).collect())

# COMMAND ----------

rdd = sc.parallelize((("A",1),("A",3),("B",4),("B",2),("C",5)))  
rdd_collect = rdd.collect()
sorted(rdd.groupByKey().mapValues(len).collect())

# COMMAND ----------

rdd = sc.parallelize((("A",1),("A",3),("B",4),("B",2),("C",5)))  
rdd_collect = rdd.collect()
sorted(rdd.groupByKey().mapValues(sum).collect())

# COMMAND ----------

# DBTITLE 1,Reduce By key
#reduceByKey() transformation is used to merge the values of each key using an associative reduce function
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [('Project', 1),
('Gutenberg’s', 1),
('Alice’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1),
('Adventures', 1),
('in', 1),
('Wonderland', 1),
('Project', 1),
('Gutenberg’s', 1)]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.reduceByKey(lambda a,b: a+b)
for element in rdd2.collect():
    print(element)

# COMMAND ----------

# DBTITLE 1,Sort by key
#Spark sortByKey() transformation is an RDD operation that is used to sort the values of the key by ascending or descending order
tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
sc.parallelize(tmp2).sortByKey(True,keyfunc=lambda string: string.lower()).collect()

# COMMAND ----------

# DBTITLE 1,aggregateByKey
#Aggregate the values of each key, using given combine functions and a neutral “zero value”.
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2),("b", 4)])
seqFunc = (lambda x, y: (x[0] + y, x[1] + 1))
combFunc = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
sorted(rdd.aggregateByKey((0, 0), seqFunc,combFunc).collect())

# COMMAND ----------

# DBTITLE 1,Join
myDict = {"name": "John", "country": "Norway"}
mySeparator = "TEST"

x = mySeparator.join(myDict)

print(x)

# COMMAND ----------

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Inner join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)
#here 2 table empDF , Dept DF join with common data as Dept_id

# COMMAND ----------

# DBTITLE 1,Outer join /fuller join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer").show(truncate=False)
#here all the available columns in both the table join print as null value when data not present in any column

# COMMAND ----------

# DBTITLE 1,Left join or left outer join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left").show(truncate=False)
#here all the data which is present in empDF table join with deptDF table 

# COMMAND ----------

# DBTITLE 1,right join or right outer join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right").show(truncate=False)
#here all the data which is present in deptDF table join with deptDF table 

# COMMAND ----------

# DBTITLE 1,Left Semi Join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftsemi").show(truncate=False)
#leftsemi join is similar to inner join difference being leftsemi join returns all common columns from the left dataset and ignores all columns from the right datase

# COMMAND ----------

# DBTITLE 1,Left Anti Join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftanti").show(truncate=False)
#leftanti join does the exact opposite of the leftsemi, leftanti join returns only columns from the left dataset for non-matched records.

# COMMAND ----------

# DBTITLE 1,coalesce and repartition spark
#The repartition algorithm does a full shuffle and creates new partitions with data that's distributed evenly.
rdd2 = rdd.repartition(4)
print("Repartition size : "+str(rdd2.getNumPartitions()))

# COMMAND ----------


# coalesce uses existing partitions to minimize the amount of data that's shuffled. 
rdd1= rdd.coalesce(4)
print("Repartition size : "+str(rdd1.getNumPartitions()))

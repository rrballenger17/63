import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp") 
sc = SparkContext(conf = conf)

# create SQLContext with the SparkContext parameter
sqlContext = SQLContext(sc)

# RDD from importing file emps.txt
emps = sc.textFile("/user/cloudera/emps.txt")

# splitting on the commas to make 3 individual elements
emps_fields = emps.map(lambda line: (line.split(", ")))

# create rows
employees = emps_fields.map(lambda e: Row(name = e[0], age = int(e[1]), salary = float(e[2])))

# create a dataframe with the rowed data
empsDF = sqlContext.createDataFrame(employees)

# show the dataframe
empsDF.show()

# create a temporary table
empsDF.registerTempTable("emps")

# select employees with salaries > 3500
sqlContext.sql("select name from emps where salary > 3500").show()

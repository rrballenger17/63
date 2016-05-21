import re
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("MyApp") 
sc = SparkContext(conf = conf)
theFile = sc.textFile("/user/cloudera/ulysses/4300.txt")

# split the line by space and map to (word, 1)
# also convert to lowercase and eliminate punctuation
counts = theFile.flatMap(lambda line: line.split(" ")).map(lambda word: (re.sub("[^A-Za-z0-9]", "", word.lower()), 1))

# reduce the mapped data and sum instances of words
counts = counts.reduceByKey(lambda a, b: a +b)

# filter out the empty string
counts = counts.filter(lambda (a,b): a!="")


# save to file
counts.saveAsTextFile("pycounts4")




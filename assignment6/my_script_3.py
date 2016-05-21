import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row, functions

#pyspark.sql.functions

conf = SparkConf().setMaster("local").setAppName("MyApp") 
sc = SparkContext(conf = conf)
theFile = sc.textFile("/user/cloudera/ebay.csv")

# create SQL context
sqlContext = SQLContext(sc)

ebay_fields = theFile.map(lambda line: (line.split(",")))

# create rows from the ebay fields
ebay_rows = ebay_fields.map(lambda e: Row(auctionid = int(e[0]),
                                          bid = float(e[1]),
                                          bidtime = float(e[2]),
                                          bidder=e[3],
                                          bidderrate=float(e[4]),
                                          openbid=float(e[5]),
                                          price=float(e[6]),
                                          item=e[7],
                                          daystolive=float(e[8])
                                          ))

# create the dataframe for the ebay data
Auction = sqlContext.createDataFrame(ebay_rows)

# print the schema of the dataframe
Auction.printSchema()

#####
# Dataframe queries 

# How many auctions were held?
print "\nNumber of auctions: " + str(Auction.select(Auction.auctionid).distinct().count())

# How many bids were made per item?
Auction.groupBy("item").agg({"bid":"count"}).show()

# minimum, maximum, and average bid (price) per item
maxP = Auction.groupBy("item").agg({"price":"max"})
minP = Auction.groupBy("item").agg({"price":"min"})
avgP = Auction.groupBy("item").agg({"price":"avg"})

combo = maxP.join(minP, maxP.item == minP.item, 'outer').drop(minP.item)
combo = combo.join(avgP, combo.item == avgP.item, 'outer').drop(avgP.item)
combo.select("item", "max(price)", "min(price)", "avg(price)").show()

# bids with price > 100
Auction.filter(Auction.bid > 100.0).show()


######
#SQL queries

Auction.registerTempTable("Auction")

#number of auctions
sqlContext.sql("select count(distinct(auctionid)) from Auction").show()

# number of bids per item
sqlContext.sql("select item, count(*) from Auction Group By item").show()

# minimum, maximum, and average bid (price) per item
sqlContext.sql("select item, max(price) as Max, min(price) as Min, avg(price) as Avg from Auction Group By item").show()

# bids > 100
sqlContext.sql("select * from Auction where bid > 100.0").show()


#####
# Parquet File storage and access

Auction.write.save("auction.parquet",format="parquet")

Auction2 = sqlContext.read.parquet("auction.parquet")

Auction2.registerTempTable("Auction2")

sqlContext.sql("select * from Auction2").show(3)














"""
 Counts words in UTF8 encoded, '\n' delimited text directly received from Kafka in every 2 seconds.
 Usage: direct_kafka_wordcount.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      examples/src/main/python/streaming/direct_kafka_wordcount.py \
      localhost:9092 test`
"""

import sys
import Queue

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils



if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", sys.stderr)
        exit(-1)


    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")

    # reduces logging significantly
    logging = sc._jvm.org.apache.log4j
    logging.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logging.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

    ssc = StreamingContext(sc, 5)

    ssc.checkpoint("file:///home/cloudera/checkpoint")

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    lines = kvs.map(lambda xtor: xtor[1])

    # added reduce by key and window
    counts = lines.flatMap(lambda linetor: linetor.split(" ")) \
        .map(lambda wordtor: (wordtor, 1)) \
        .reduceByKeyAndWindow((lambda ator, btor: ator+btor), (lambda ator, btor: ator-btor), 30,5)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()












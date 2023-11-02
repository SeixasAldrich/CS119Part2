import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: goog-msft-processor.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="PythonStreamingStockProcessor")
    ssc = StreamingContext(sc, 1)

    rawPrices = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    priceDate = rawPrices.map(lambda l: (l.split(" ")(0)))

    googPrice = rawPrices.map(lambda l: (l.split(" ")(1)))\
                         .join(priceDate)
    msftPrice = rawPrices.map(lambda l: (l.split(" ")(1)))\
                         .join(priceDate)
 
    googPrice.pprint()
    msftPrice.pprint()

    ssc.start()
    ssc.awaitTermination()
  

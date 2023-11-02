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

    googPrice = rawPrices.map(lambda l: (l.split(" ")(0), l.split(" ")(1)))\
                         .foldLeft("")(lambda x, y: (x+" "+y))
    msftPrice = rawPrices.map(lambda l: (l.split(" ")(0), l.split(" ")(2)))\
                         .foldLeft("")(lambda x, y: (x+" "+y))
 
    googPrice.pprint()
    msftPrice.pprint()

    ssc.start()
    ssc.awaitTermination()
  

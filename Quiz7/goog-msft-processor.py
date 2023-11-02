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

    googPrice = rawPrices.map(lambda l: float(l.split(" ")[1]))
    # msftPrice = rawPrices.map(lambda l: float(l.split(" ")[2]))

    goog10Day = googPrice.window(10,1)\
                         .reduce(lambda x, y: (x + y) / 2.0)\
                         .map(lambda m: "10 day average: " + tr(m))
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .filter()
                         # .map(lambda m: "10 day average: " + tr(m))
    goog40Day = googPrice.window(40,1)\
                         .reduce(lambda x, y: (x + y) / 2.0)\
                         .map(lambda m: "40 day average: " + tr(m))
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .map(lambda m: "40 day average: " + tr(m))

    # msft10Day = msftPrice.window(10,1)\
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .filter()
                         # .map(lambda m: "10 day average: " + tr(m))
    # msft40Day = msftPrice.window(40,1)\
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .map(lambda m: "40 day average: " + tr(m))
 
    goog10Day.pprint()
    goog40Day.pprint()
    # msftPrice.pprint()

    ssc.start()
    ssc.awaitTermination()
  

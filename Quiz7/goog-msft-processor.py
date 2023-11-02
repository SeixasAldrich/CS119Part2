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

    date = rawPrices.map(lambda l: float(l.split(" ")[0]))

    googPrice = rawPrices.map(lambda l: float(l.split(" ")[1]))
    # msftPrice = rawPrices.map(lambda l: float(l.split(" ")[2]))

    goog10Day = googPrice.window(10,1)\
                         .reduce(lambda x, y: (x + y) / 2.0)\
                         .map(lambda m: "10 day average: " + str(m))
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .filter()
                         # .map(lambda m: "10 day average: " + str(m))
    goog40Day = googPrice.window(40,1)\
                         .reduce(lambda x, y: (x + y) / 2.0)\
                         .map(lambda m: "40 day average: " + str(m))
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .map(lambda m: "40 day average: " + str(m))

    # msft10Day = msftPrice.window(10,1)\
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .filter()
                         # .map(lambda m: "10 day average: " + str(m))
    # msft40Day = msftPrice.window(40,1)\
                         # .map(lambda price: (price, 1))\
                         # .reduce(lambda x, y: (x + y) / 2.0)\
                         # .map(lambda m: "40 day average: " + str(m))

    bOrsYestgoog = bOrs = goog10Day.window(2,1)[1].join(goog40Day.window(2,1)[1]).reduce(lambda x, y: 1 if x > y else 0)


    bOrsgoog = goog10Day.window(2,1)[0].join(goog40Day.window(2,1)[0]).reduce(lambda x, y: 1 if x > y else 0)

    if (bOrsgoog == 1 && bOrsYestgoog == 0):
        print(date + "buy" + goog)
    elif (bOrsgoog == 0 && bOrsYestgoog == 1):
        print(date + "sell" + goog)
        
 
    # goog10Day.pprint()
    # goog40Day.pprint()
    # msftPrice.pprint()

    ssc.start()
    ssc.awaitTermination()
  

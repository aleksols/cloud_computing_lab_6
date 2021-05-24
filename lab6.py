from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

STREAM_TYPE = "socket"


def get_stream(ssc):
    if STREAM_TYPE == "socket":
        return ssc.socketTextStream("localhost", 9999)
    return ssc.textFileStream("tweets")


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process_rdd(time, rdd):
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        row = rdd.map(lambda w: Row(hashtag=w[0], number=w[1]))
        df = spark.createDataFrame(row)
        df.createOrReplaceTempView("hashtags")
        result_df = spark.sql(
            "select hashtag, number from hashtags order by number desc limit 10"
        )
        print("----------- %s -----------" % str(time))
        result_df.show()
    except:
        pass


def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


sc = SparkContext(appName="S&PCorrelation")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 3)
ssc.checkpoint("checkpoint_S&PCorrelation")


def updateFunc(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)


tweets = get_stream(ssc)

# Use upper to caputre #sp500 as well
tweets = tweets.map(lambda tweet: tweet.upper())
tweets = tweets.filter(lambda tweet: "#SP500" in tweet)
words = tweets.flatMap(lambda line: line.split(" "))
words = words.filter(lambda w: "#" in w and w != "#SP500")
words = words.map(lambda w: w.upper())
words = words.map(lambda x: (x, 1))

# results = words.updateStateByKey(updateFunc=updateFunc)
results = words.updateStateByKey(
    lambda new_values, last_sum: sum(new_values) + (last_sum or 0)
)

results.foreachRDD(process_rdd)


ssc.start()
ssc.awaitTermination()

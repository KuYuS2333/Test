import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from IPython.display import clear_output
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import time

# 从kafka获取标题
@udf(returnType=StringType())
def gettitle(column):
    #return str(json.loads(column)["title"])
    jsonobject = json.loads(column)
    jsonobject = json.loads(jsonobject)
    #return str(jsonobject)
    if "title" in jsonobject:
        return str(jsonobject['title'])
    return ""

# 获取情感分析得分
@udf(returnType=DoubleType())
def getscore(column):
    #return str(json.loads(column)["title"])
    jsonobject = json.loads(column)
    jsonobject = json.loads(jsonobject)
    #return str(jsonobject)
    if "sentiment_score" in jsonobject:
        return float(jsonobject['sentiment_score'])
    return 0.5

if __name__ == "__main__":
    # Checking validity of Spark submission command
    if len(sys.argv) != 4:
        print("Wrong number of args.", file=sys.stderr)
        sys.exit(-1)
    # Initializing Spark session
    spark = SparkSession\
        .builder\
        .appName("WeiboSpark")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    # Setting parameters for the Spark session to read from Kafka
    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]
    # Streaming data from Kafka topic as a dataframe
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, topics)\
        .load()
    lines.printSchema()

    kafka_value_tb = lines.selectExpr("CAST(value AS STRING) as json", "timestamp")
    weibo_table = kafka_value_tb.select(gettitle(col("json")).alias("title"),
        getscore(col("json")).alias("sentiment_score"), col("timestamp"))
    stat_avg = weibo_table.groupBy(window(col("timestamp"), "30 seconds", "10 seconds"), col("title")).avg("sentiment_score").where("unix_timestamp(window.end)=int(unix_timestamp(current_timestamp)/10)*10")
    
    queryStream = (stat_avg.writeStream.format("memory").queryName("weibotop").outputMode("complete").start())
    try:
        i=1
        while True:
            print('count ', str(i))
            df = spark.sql(
                    """
                        select * from weibotop 
                    """
            ).toPandas()
            #df = df.sort_values(by="window",ascending=False)
            print(df)
            df.to_csv("weibo_sentiment_result.csv")
            
            time.sleep(10)
            i=i+1
    except KeyboardInterrupt:
        print("process interrupted.")

    queryStream.awaitTermination()



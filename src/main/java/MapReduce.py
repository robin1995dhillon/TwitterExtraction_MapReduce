from pymongo import MongoClient
import operator
from pyspark import SparkContext
import pyspark
from pymongo import MongoClient
from pyspark.sql import SparkSession

print("start")
client = MongoClient("mongodb+srv://robindermongo:root@cluster0.hon6x.mongodb.net/test")
processedDb = client['ProcessedDb']
reutersDb = client['ReutersDb']
processedData = processedDb['ProcessedData']
reutersData = reutersDb['ReutersData']
trackList = ["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety", "hot", "cold", "rain", "ice"]
processedList = []
dictionary = {}

for lists in processedData.find():
    processedList.append(lists)

for listReuters in reutersData.find():
    processedList.append(listReuters)

processedList = spark.sparkContext.parallelize(processedList)
all_lines = processedList.flatMap(lambda line: line.split(" "))
all_words = all_lines.map(lambda word: (word,1))
our_words = all_words.filter(lambda word: word in trackList)
words_count = our_words.reduceByKey(operator.add)
print(words_count.collect())

from csv import reader
from pyspark import SparkContext

#sc = SparkContext(appName="hw2")
sc.setLogLevel("ERROR")

# read input data from HDFS to create an RDD
data = sc.textFile("hdfs://group13-1:54310/hw1-input/")

# use csv reader to split each line of file into a list of elements.
# this will automatically split the csv data correctly.
splitdata = data.mapPartitions(lambda x: reader(x))

# use filter to select only those rows in which crime type is not blank
splitdata = splitdata.filter(lambda x: x[7])

# select column BORO_NM 
splitBoro = splitdata.map(lambda line: (line[13],1))

#…rest of your code goes here…
mostCrimesNY = splitBoro.reduceByKey(lambda x,y: x + y)
topCrimesJuly = splitdata.filter(lambda line: (line[5][0:2] == "07"))
topCrimesJuly = topCrimesJuly.map(lambda line: (line[7],1)).reduceByKey(lambda x,y: x + y)

#print resu
print("Where is most of crime happening in New York?",mostCrimesNY.sortBy(lambda x: x[1],False).keys().take(1))
print("What is the total number of crimes reported in that location? ", mostCrimesNY.sortBy(lambda x: x[1],False).values().take(1))
print("What are the top 3 crimes that were reported in the month of July? ", topCrimesJuly.sortBy(lambda x: x[1],False).keys().take(3))
print("How many crimes of type DANGEROUS WEAPONS were reported in the month of July? ", topCrimesJuly.filter(lambda line: (line[0] == "DANGEROUS WEAPONS")).values().take(1))
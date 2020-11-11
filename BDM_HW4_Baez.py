from pyspark.sql import SparkSession
import sys

if __name__=='__main__':
	ss = SparkSession\
		.builder\
		.master("yarn")\
		.appName("Consumer Complaints")\
		.getOrCreate;
	
	df = ss.read.csv(sys.argv[1], header=True, inferSchema=True, multiLine=True)
	df.head(5) 

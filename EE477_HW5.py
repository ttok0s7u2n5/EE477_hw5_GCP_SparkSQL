#
#####################################################################################################################
"""                                  EE477 Database and Bigdata system  HW5                                        """
#######################################################################################################################
"""
   * pyspark rdd methods : https://spark.apache.org/docs/1.1.1/api/python/pyspark.rdd.RDD-class.html
"""
import sys
from pyspark.sql import SparkSession


def warmup_sql (spark, datafile):
   
    df = spark.read.parquet(datafile)
    df.createOrReplaceTempView("flights")
    rq = spark.sql("SELECT depdelay FROM flights LIMIT 100")
    
    rq.show()
    
    return rq

def warmup_rdd (spark, datafile):
   
    d = spark.read.parquet(datafile).rdd
    
    r = d.map(lambda x: x['depdelay'])
    r = spark.sparkContext.parallelize(r.take(100)) # limit 100
    
    return r

def Q1 (spark, datafile):
    
    df = spark.read.parquet(datafile)
    
    # your code here
    df.createOrReplaceTempView("flights")
    rq = spark.sql("SELECT DISTINCT f.destcityname FROM flights f WHERE origincityname = 'Seattle, WA'")
    
    rq.show()
    
    return rq

def Q2 (spark, datafile):
    
    d = spark.read.parquet(datafile).rdd
    
    # your code here
    r = d.filter(lambda x: x['origincityname'] == 'Seattle, WA') \
    .map(lambda x: x['destcityname']) \
    .distinct()

    return r;
    
def Q3 (spark, datafile):
    
    d = spark.read.parquet(datafile).rdd
    
    # your code here
    r = d.filter(lambda x: x['cancelled'] == 0) \
    .map(lambda x: ((x['origincityname'], x['month']), 1)) \
    .reduceByKey(lambda a, b: a + b)

    return r

def Q4 (spark, datafile):
 
    d = spark.read.parquet(datafile).rdd
    
    # your code here
    r = d.map(lambda x: (x['destcityname'], x['origincityname'])).distinct() \
    .map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b) \
    .takeOrdered(1, key = lambda x: -x[1])
    r = spark.sparkContext.parallelize(r)
    
    return r

def Q5 (spark, datafile):
    
    d = spark.read.parquet(datafile).rdd
    
    # your code here
    r = d.filter(lambda x: x['depdelayminutes'] != None) \
    .map(lambda x: (x['origincityname'], (x['depdelayminutes'], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: (1.0 * v[0]) / (1.0 * v[1]))
    
    return r

if __name__ == '__main__':
    
    input_data = sys.argv[1]
    output = sys.argv[2]
    
    spark = SparkSession.builder.appName("HW5").getOrCreate()

    """ - WarmUp SQL - """
    #rq = warmup_sql(spark, input_data)
    #rq.rdd.repartition(1).saveAsTextFile(output)
    
    """ - WarmUp RDD - """
    #r = warmup_rdd(spark, input_data)
    #r.repartition(1).saveAsTextFile(output)
    
    """ - Problem 1 - """
    #rq1 = Q1(spark, input_data)
    #rq1.rdd.repartition(1).saveAsTextFile(output)
    
    """ - Problem 2 - """
    #r2 = Q2(spark, input_data)
    #r2.repartition(1).saveAsTextFile(output)
    
    """ - Problem 3 - """
    #r3 = Q3(spark, input_data)
    #r3.repartition(1).saveAsTextFile(output)
    
    """ - Problem 4 - """
    
    #r4 = Q4(spark, input_data)
    #r4.repartition(1).saveAsTextFile(output)
    
    """ - Problem 5 - """
    r5 = Q5(spark, input_data)
    r5.repartition(1).saveAsTextFile(output)

    spark.stop()

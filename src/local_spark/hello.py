from pyspark.sql import SparkSession

# SPARK_HOME=/opt/homebrew/Cellar/apache-spark/3.5.2 
# PYSPARK_PYTHON=/usr/bin/python3
# spark-submit hello.py 
# export JAVA_HOME=$(/usr/libexec/java_home -v 17)


# ipconfig getifaddr en0
# https://github.com/bitnami/containers/issues/25805

def main():
    # Initialize SparkSession
    # spark = SparkSession.builder \
    #     .appName("HelloWorld") \
    #     .master("spark://spark:7077") \
    #     .getOrCreate()

    from pyspark import SparkContext, SparkConf 
    appName = "testsparkapp" 
    master = "spark://localhost:7077"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf) 

    # Create an RDD containing numbers from 1 to 10
    numbers_rdd = sc.parallelize(range(1, 1000))

    # Count the elements in the RDD
    count = numbers_rdd.count()
    print("ok")

    print(f"Count of numbers from 1 to 1000 is: {count}")

    # Stop the SparkSession
    sc.stop()


if __name__ == "__main__":
    print("START")
    main()
    print("END")
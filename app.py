from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/Users/ismetsari/Desktop/spark-migration-exercise/postgresql-42.5.0.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
    .option("password", "somePassword") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()


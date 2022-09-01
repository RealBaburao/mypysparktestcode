from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone","EST").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
#adding config("spark.sql.session.timeZone","EST") into spark session will change the EST timezonw while parsing date and time in this spark session
data="C:\\bigdata\\datasets\\us-500.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

ndf=df.select("first_name","last_name")
# ndf.printSchema()
ndf.show()

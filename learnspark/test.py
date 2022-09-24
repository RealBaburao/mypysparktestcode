from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

import pandas

data = "C:\\bigdata\\datasets\\zips.json"

df=spark.read.format('json').load(data)
import re
res=[re.sub("[^a-zA-Z1-9]","",c.lower()) for c in df.columns]
# ndf=df.toDF(*res).withColumn("loc",explode(col("loc")))

ndf=df.toDF(*res).withColumn("langi",col("loc")[0]).withColumn("lati",col("loc")[1]).drop("loc")
ndf.createOrReplaceTempView("tab")
ndf1=spark.sql("select * from tab where state='CA'")
ndf1.printSchema()
ndf1.show(truncate=False)

host="jdbc:mysql://mysqldb.czvppjuvlzb7.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
uname="myuser"
upass="mypassword"
ndf1.write.format("jdbc").option("url",host).option("dbtable","abcd")\
    .option("user",uname).option("password",upass)\
    .option("driver","com.mysql.jdbc.Driver").save()

# output_path="C:\\bigdata\\datasets\\output_path\\result_json"
# ndf.write.mode("append").format("csv").option("header","true").save(output_path)

#simple data types:- string, int, double, date etc.
#complex data types:- struct, map,array etc
#explode() will simply create different records with each element in the array datatype. i.e 1st element 1 record and 2nd element 2nd record and so on
#mode("append") or mode("overwrite") to append data in existing file or replace the output file
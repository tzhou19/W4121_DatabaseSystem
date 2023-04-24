import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode, lower, count, lit, countDistinct
from pyspark.sql.types import StringType, ArrayType

print("finish importing")


spark = SparkSession.builder.getOrCreate()
df_test = spark.read.format('xml').options(rowTag='page').load('hdfs:/wiki-small.xml')



def get_link(text):
    regex = '\[\[(.*?)\]\]'
    matches = re.findall(regex, text)
    result = []
    for match in matches:
        match = match.split('|')[0]
        if '#' in match:
            continue
        if ':' in match:
            if match.startswith('Category:'):
                result.append(match.lower())
            else:
                continue
        result.append(match.lower())
                          
    return result

print("finish defining")


get_link_UDF = udf(lambda z: get_link(z), ArrayType(StringType()))
print("step 1")
df_temp = df_test.select(col("title"), get_link_UDF(lower(col("revision.text._VALUE"))).alias("title2"))
print("step 2")
df_link_small = df_temp.select(lower(df_temp.title).alias("Title"), explode(df_temp.title2).alias("LinkedArtical"))
print("step 3")
df_link_small = df_link_small.orderBy(['Title', 'LinkedArtical'], ascending=[1,1])






import pyspark
import os
from user_definition import *



def func(x):
    if len(x) == 6:
        return x[0:3] + x[4:]
    else:
        return x


def indexed(x):
    return [x[0], x[1], int(x[4])]



conf = (pyspark.SparkConf()
        .set('spark.driver.memory ', '10g')
        .set('spark.executor.memory ', '10g')
        .setAppName(app_name))


sc = pyspark.SparkContext.getOrCreate(conf=conf)

# sc = pyspark.SparkContext.getOrCreate()

sc._jsc.hadoopConfiguration().\
    set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

sc._jsc.hadoopConfiguration().\
    set('fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])

sc._jsc.hadoopConfiguration().\
    set('fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])

data = sc.textFile(input_file)

# Q_1
new_rdd = data.map(lambda x: x.split('\t'))\
    .map(lambda x: func(x)).map(lambda x: "\t".join(x)).distinct()
print(new_rdd.count())
print()

# Q2
final_rdd = new_rdd.map(lambda x: x.split('\t'))
first = final_rdd.sortBy(lambda x: x[0]).first()[0]
print(first)
last = final_rdd.sortBy(lambda x: x[0], ascending=False).first()[0]
print(last)
print()
# Q3
lst = final_rdd.map(lambda x: x[3]).distinct().sortBy(lambda x: x).collect()
for ls in lst:
    print(ls)
print()

# Q4
dicti = final_rdd.map(lambda x: int(x[4])).sortBy(lambda x: x).countByValue()
for val in dicti:
    print(f"{val} - {dicti[val]}")
print()

# Q5
avg_pop = final_rdd.filter(lambda x: x[3] == country_name).\
    map(lambda x: int(x[4])).mean()

avg_rdd = final_rdd.\
    filter(lambda x: x[3] == country_name and int(x[4]) > avg_pop)\
    .map(lambda x: indexed(x))  # .sortBy(lambda x:(int(x[2]),x[1]).collect()
lst = avg_rdd.sortBy(lambda x: (x[2], x[0], x[1])).take(n)

for ls in lst:
    ls = [str(ele) for ele in ls]
    string = ", ".join(ls)
    print(string)
print()

# Q6
lst = final_rdd.filter(lambda x: int(x[4]) == score).\
    sortBy(lambda x: (x[0], x[3], x[1]), ascending=(True, True, True))\
    .map(lambda x: x[0:4]).take(n)
for ls in lst:
    string = ", ".join(ls)
    print(string)
print()

# Q7
filtered_rdd_1 = final_rdd.filter(lambda x: x[2] == str(IsImplicitIntent_val))
filtered_rdd_2 = filtered_rdd_1.filter(lambda x: x[3] == country_name)
filtered_rdd = filtered_rdd_2.filter(lambda x: x[1].startswith(query_startswith))

lst = filtered_rdd.map(lambda x: x[1]).distinct().\
    sortBy(lambda x: x).collect()
string = ", ".join(lst)
print(string)
print()

sc.stop()

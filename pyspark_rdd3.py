from user_definition import *
from pyspark import SparkContext
import pyspark


conf = pyspark.SparkConf().set("spark.driver.host", "localhost")
sc = SparkContext.getOrCreate()
sc.setLogLevel('OFF')


input_rdd = sc.textFile(f"{input_folder}/*.tsv")

first_ = input_rdd.map(lambda x: x.split('\t')[0]).first()

new_rdd = input_rdd.filter(lambda x: x.split(
    '\t')[0] != first_).map(lambda x: x.split('\t'))
# Q1
count_15 = new_rdd.map(lambda x: len(x)).filter(lambda x: x == 15).count()
print(count_15)

print()
# Q2
count_8 = new_rdd.map(lambda x: len(x)).filter(lambda x: x == 8).count()
print(count_8)

print()
# Q3

key_rdd_15 = new_rdd.filter(lambda x: len(
    x) == 15).map(lambda x: (x[0], x[1:]))
key_rdd_8 = new_rdd.filter(lambda x: len(x) == 8).map(lambda x: (x[0], x[1:]))

lst = key_rdd_15.subtractByKey(key_rdd_8).map(
    lambda x: [x[1][1], x[0], x[1][0]]).\
    sortBy(lambda x: (x[1], x[0], x[2])).collect()

for ele in lst:
    print(f"{ele[0]}, {ele[1]} - {ele[2]}")

print()
# Q4
lst = new_rdd.map(lambda x: (x[0], 1)).groupByKey().mapValues(
    lambda x: sum(x)).sortByKey().collect()
for ele in lst:
    print(f"{ele[0]} - {ele[1]}")

print()
# Q5


def func(x):
    if len(x) == 15:
        new_lst = [ele if ele != "" else '0' for ele in x]
        int_list = [int(ele.replace('"', '').replace(',', ''))
                    for ele in new_lst[3:]]
        return (new_lst[0], new_lst[1], int_list)
    else:
        new_lst = [ele if ele != "" else '0' for ele in x]
        int_list = [int(ele.replace('"', '').replace(',', ''))
                    for ele in new_lst[3:]]
        return (x[0], x[1], [0]*7 + int_list)


lst = new_rdd.filter(lambda x: x[2] == city_name).map(
    lambda x: func(x)).sortBy(lambda x: (x[0], x[1])).collect()
for ele in lst:
    print(f"{ele[0]}, {ele[1]} - {ele[2]}")

print()
# Q6


def sum_func(x, y):
    c = [0]*len(x)
    for i in range(len(x)):
        c[i] += x[i] + y[i]
    return c


rdd_1 = new_rdd.filter(lambda x: x[0] == state_name).map(
    lambda x: func(x)).map(lambda x: (x[0], x[2]))

lst = rdd_1.reduceByKey(lambda x, y: sum_func(x, y)).collect()

for ele in lst:
    print(f"{ele[0]} - {ele[1]}")

print()
# Q7


def func_new(x):
    if len(x) == 15:
        new_lst = [ele if ele not in ("", ' ') else '0' for ele in x]
        int_list = [int(ele.replace('"', '').replace(
            ',', '').replace(' ', '')) for ele in new_lst[3:]]
        return [new_lst[0], new_lst[1], new_lst[2], int_list]
    else:
        new_lst = [ele if ele not in ("", ' ') else '0' for ele in x]
        int_list = [int(ele.replace('"', '').replace(
            ',', '').replace(' ', '')) for ele in new_lst[3:]]
        return [x[0], x[1], x[2], [0]*7 + int_list]


lst = new_rdd.map(lambda x: func_new(x)).sortBy(
    lambda x: x[3][index_no-4], ascending=False).\
    map(lambda x: ((x[0], x[1], x[2]), x[3][index_no-4])).take(1)
print(f"{lst[0][0]} - {lst[0][1]}")


sc.stop()

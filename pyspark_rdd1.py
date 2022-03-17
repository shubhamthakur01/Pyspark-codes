import pyspark
import os
import datetime

from user_definition import *
conf = pyspark.SparkConf().set("spark.driver.host", "localhost")
sc = pyspark.SparkContext(appName=app_name, conf=conf)
sc.setLogLevel('OFF')


def fn(lst):
    for i in lst:
        print(i)
    # return length


def filt(x):
    if x not in lst_distinct_state:
        return True
    else:
        return False


def main():
    input_rdd = sc.textFile(input_file, partition_ct)
    count_rdd = input_rdd. map(lambda x: x.split(',')).map(lambda x: len(x))
    new = input_rdd.mapPartitions(lambda x: len(x))
    # Q1
    lst = input_rdd.glom().map(lambda x: len(x)).collect()
    fn(lst)
    print()
    # Q2
    new_rdd = input_rdd.map(lambda x: x.split('\n')).\
        map(lambda x: x[0].replace('"', '').split(','))
    lst = new_rdd.collect()[0]
    fn(lst)
    print()
    # Q3
    lst = new_rdd.map(lambda x: x != lst).collect()
    print(sum(lst))
    print()
    # Q4
    lst = new_rdd.filter(lambda x: x[0] == state_name).\
        sortBy(lambda x: x[1]).collect()
    fn(lst)
    print()
    # Q5
    lst = new_rdd.collect()[0]
    lst = new_rdd.filter(lambda x: x != lst).map(lambda x: x[1])
    .map(lambda x: datetime.datetime.strptime(
        x, '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S'))
    .distinct().sortBy(lambda x: x).collect()
    fn(lst)
    print()
    # Q6
    lst = new_rdd.collect()[0]
    lst_distinct_state = new_rdd.\
        filter(lambda x: x != lst and x[0] == state_name).\
        map(lambda x: x[1]).distinct().sortBy(lambda x: x).collect()
    lst = new_rdd.collect()[0]
    lst_distinct_missing = new_rdd.\
        filter(lambda x: x != lst).\
        map(lambda x: x[1]).distinct().\
        sortBy(lambda x: x).filter(lambda x: filt(x)).\
        map(lambda x: datetime.datetime.
            strptime(x, '%Y-%m-%dT%H:%M:%S.%f').
            strftime('%Y-%m-%d %H:%M:%S')).collect()
    fn(lst_distinct_missing)
    print()


main()
sc.stop()

import os
import sys
import copy
import time
import random
import pyspark
from statistics import mean
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import desc, size, max, abs, col, sum
'''
INTRODUCTION
With this assignment you will get a practical hands-on of frequent 
itemsets and clustering algorithms in Spark. Before starting, you may 
want to review the following definitions and algorithms:
* Frequent itemsets: Market-basket model, association rules, confidence, interest.
* Clustering: kmeans clustering algorithm and its Spark implementation.
DATASET
We will use the dataset at 
https://archive.ics.uci.edu/ml/datasets/Plants, extracted from the USDA 
plant dataset. This dataset lists the plants found in US and Canadian 
states.
The dataset is available in data/plants.data, in CSV format. Every line 
in this file contains a tuple where the first element is the name of a 
plant, and the remaining elements are the states in which the plant is 
found. State abbreviations are in data/stateabbr.txt for your 
information.
'''
'''
HELPER FUNCTIONS
These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
return spark
def toCSVLineRDD(rdd):
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
return a + os.linesep
def toCSVLine(data):
if isinstance(data, RDD):
if data.count() > 0:
return toCSVLineRDD(data)
else:
return ""
elif isinstance(data, DataFrame):
if data.count() > 0:
return toCSVLineRDD(data.rdd)
else:
return ""
return None
all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
"ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
"ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
"ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
"tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
"bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
"yt", "dengl", "fraspm" ]
'''
PART 1: FREQUENT ITEMSETS
Here we will seek to identify association rules between states to 
associate them based on the plants that they contain. For instance, 
"[A, B] => C" will mean that "plants found in states A and B are likely 
to be found in state C". We adopt a market-basket model where the 
baskets are the plants and the items are the states. This example 
intentionally uses the market-basket model outside of its traditional 
scope to show how frequent itemset mining can be used in a variety of 
contexts.
'''
def data_frame(filename, n):
'''
    Write a function that returns a CSV string representing the first 
    <n> rows of a DataFrame with the following columns,
    ordered by increasing values of <id>:
    1. <id>: the id of the basket in the data file, i.e., its line number - 1 (ids start at 0).
    2. <plant>: the name of the plant associated to basket.
    3. <items>: the items (states) in the basket, ordered as in the data file.
    Return value: a CSV string. Using function toCSVLine on the right 
                  DataFrame should return the correct answer.
    Test file: tests/test_data_frame.py
    '''
    spark = init_spark()
    lines = spark.read.text(filename).rdd
    parts = lines.map(lambda row: row.value.split(',')).zipWithIndex()
    plants_with_states = parts.map(lambda p: Row(index=int(p[1]), name=p[0][0], states=p[0][1:]))
    plants_df = spark.createDataFrame(plants_with_states).limit(n)
return toCSVLine(plants_df)
def frequent_itemsets(filename, n, s, c):
'''
    Using the FP-Growth algorithm from the ML library (see 
    http://spark.apache.org/docs/latest/ml-frequent-pattern-mining.html), 
    write a function that returns the first <n> frequent itemsets 
    obtained using min support <s> and min confidence <c> (parameters 
    of the FP-Growth model), sorted by (1) descending itemset size, and 
    (2) descending frequency. The FP-Growth model should be applied to 
    the DataFrame computed in the previous task. 
    
    Return value: a CSV string. As before, using toCSVLine may help.
    Test: tests/test_frequent_items.py
    '''
    spark = init_spark()
    lines = spark.read.text(filename).rdd
    parts = lines.map(lambda row: row.value.split(','))
    plants_with_states = parts.map(lambda p: Row(name=p[0], states=p[1:]))
    plants_df = spark.createDataFrame(plants_with_states)
    fpGrowth = FPGrowth(itemsCol="states", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(plants_df)
    freq_itemsets_df = model.freqItemsets
return toCSVLine(freq_itemsets_df.orderBy(size(freq_itemsets_df.items).desc(), freq_itemsets_df.freq.desc()).limit(n))
def association_rules(filename, n, s, c):
'''
    Using the same FP-Growth algorithm, write a script that returns the 
    first <n> association rules obtained using min support <s> and min 
    confidence <c> (parameters of the FP-Growth model), sorted by (1) 
    descending antecedent size in association rule, and (2) descending 
    confidence.
    Return value: a CSV string.
    Test: tests/test_association_rules.py
    '''
    spark = init_spark()
    lines = spark.read.text(filename).rdd
    parts = lines.map(lambda row: row.value.split(','))
    plants_with_states = parts.map(lambda p: Row(name=p[0], states=p[1:]))
    plants_df = spark.createDataFrame(plants_with_states)
    fpGrowth = FPGrowth(itemsCol="states", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(plants_df)
    result_df = model.associationRules
    result_df = result_df.orderBy(size(result_df.antecedent).desc(), result_df.confidence.desc()).limit(n)
return toCSVLine(result_df.select('antecedent', 'consequent', 'confidence'))
def interests(filename, n, s, c):
'''
    Using the same FP-Growth algorithm, write a script that computes 
    the interest of association rules (interest = |confidence - 
    frequency(consequent)|; note the absolute value)  obtained using 
    min support <s> and min confidence <c> (parameters of the FP-Growth 
    model), and prints the first <n> rules sorted by (1) descending 
    antecedent size in association rule, and (2) descending interest.
    Return value: a CSV string.
    Test: tests/test_interests.py
    '''
    spark = init_spark()
    lines = spark.read.text(filename).rdd
    parts = lines.map(lambda row: row.value.split(','))
    plants_with_states = parts.map(lambda p: Row(name=p[0], states=p[1:]))
    plants_df = spark.createDataFrame(plants_with_states)
    fpGrowth = FPGrowth(itemsCol="states", minSupport=s, minConfidence=c)
    model = fpGrowth.fit(plants_df)
    freq_itemsets_df = model.freqItemsets
    association_df = model.associationRules
    result_df = freq_itemsets_df.join(association_df, freq_itemsets_df.items == association_df.consequent)
    result_df = result_df.withColumn('interest', abs(result_df.confidence - (result_df.freq/plants_df.count())))
    result_df = result_df.orderBy(size(result_df.antecedent).desc(), result_df.interest.desc()).limit(n)
    result_df = result_df.select('antecedent', 'items', 'confidence', 'consequent', 'freq', 'interest')
    result_df.show()
return toCSVLine(result_df)
'''
PART 2: CLUSTERING
We will now cluster the states based on the plants that they contain.
We will reimplement and use the kmeans algorithm. States will be 
represented by a vector of binary components (0/1) of dimension D, 
where D is the number of plants in the data file. Coordinate i in a 
state vector will be 1 if and only if the ith plant in the dataset was 
found in the state (plants are ordered alphabetically, as in the 
dataset). For simplicity, we will initialize the kmeans algorithm 
randomly.
An example of clustering result can be visualized in states.png in this 
repository. This image was obtained with R's 'maps' package (Canadian 
provinces, Alaska and Hawaii couldn't be represented and a different 
seed than used in the tests was used). The classes seem to make sense 
from a geographical point of view!
'''
# extracted to use in the rest of the questions
def state_plants_dictionary(filename):
    spark = init_spark()
    lines = spark.sparkContext.textFile(filename)
    parts = lines.map(lambda row: row.split(','))
return parts.flatMap(lambda p: [(p[0], p[state]) for state in range(1, len(p))]).map(lambda p: (p[1], {p[0]:1})).reduceByKey(lambda state, plant: {**state, **plant})
def data_preparation(filename, plant, state):
'''
    This function creates an RDD in which every element is a tuple with 
    the state as first element and a dictionary representing a vector 
    of plant as a second element:
    (name of the state, {dictionary})
    The dictionary should contains the plant names as keys. The 
    corresponding values should be 1 if the plant occurs in the state 
    represented by the tuple.
    You are strongly encouraged to use the RDD created here in the 
    remainder of the assignment.
    Return value: True if the plant occurs in the state and False otherwise.
    Test: tests/test_data_preparation.py
    '''
    spark = init_spark()
return plant in state_plants_dictionary(filename).filter(lambda p: p[0] == state).take(1)[0][1]
def distance2(filename, state1, state2):
'''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    state_dict = state_plants_dictionary(filename)
    set1 = state_dict.filter(lambda p: p[0] == state1).take(1)[0][1]
    set2 = state_dict.filter(lambda p: p[0] == state2).take(1)[0][1]
return distance_of_two_sets(set1, set2)
def distance_of_two_sets(set1, set2):
return len(set(set1) - set(set2)) + len(set(set2) - set(set1))
def init_centroids(k, seed):
'''
    This function randomly picks <k> states from the array in answers/all_states.py (you
    may import or copy this array to your code) using the random seed passed as
    argument and Python's 'random.sample' function.
    In the remainder, the centroids of the kmeans algorithm must be
    initialized using the method implemented here, perhaps using a line
    such as: `centroids = rdd.filter(lambda x: x[0] in
    init_states).collect()`, where 'rdd' is the RDD created in the data
    preparation task.
    Note that if your array of states has all the states, but not in the same
    order as the array in 'answers/all_states.py' you may fail the test case or
    have issues in the next questions.
    Return value: a list of <k> states.
    Test: tests/test_init_centroids.py
    '''
    random.seed(seed)
return random.sample(all_states, k)
def first_iter(filename, k, seed):
'''
    This function assigns each state to its 'closest' class, where 'closest'
    means 'the class with the centroid closest to the tested state
    according to the distance defined in the distance function task'. Centroids
    must be initialized as in the previous task.
    Return value: a dictionary with <k> entries:
    - The key is a centroid.
    - The value is a list of states that are the closest to the centroid. The list should be alphabetically sorted.
    Test: tests/test_first_iter.py
    '''
    init_states = init_centroids(k, seed)
    state_dict = state_plants_dictionary(filename).filter(lambda x: x[0] in all_states)
    centroids = state_dict.filter(lambda state: state[0] in init_states).collect()
    distances = state_dict.flatMap(lambda state: [(state[0], (centroid[0], len(set(state[1]) - set(centroid[1])) + len(set(centroid[1]) - set(state[1])))) for centroid in centroids])
return distances.reduceByKey(lambda d1, d2: min([d1, d2], key = lambda x: x[1])).map(lambda x: (x[1][0], [x[0]])).reduceByKey(lambda s1, s2: s1 + s2).map(lambda c: (c[0], sorted(c[1]))).collectAsMap()
def kmeans(filename, k, seed):
'''
    This function:
    1. Initializes <k> centroids.
    2. Assigns states to these centroids as in the previous task.
    3. Updates the centroids based on the assignments in 2.
    4. Goes to step 2 if the assignments have not changed since the previous iteration.
    5. Returns the <k> classes.
    Note: You should use the list of states provided in all_states.py to ensure that the same initialization is made.
    
    Return value: a list of lists where each sub-list contains all states (alphabetically sorted) of one class.
                  Example: [["qc", "on"], ["az", "ca"]] has two 
                  classes: the first one contains the states "qc" and 
                  "on", and the second one contains the states "az" 
                  and "ca".
    Test file: tests/test_kmeans.py
    '''
    spark = init_spark()
    random.seed(seed)
    sd = state_plants_dictionary(filename).filter(lambda x: x[0] in all_states)
    sdict = state_plants_dictionary(filename).filter(lambda x: x[0] in all_states).collectAsMap()
    centroids = init_centroids(k, seed)
    centroids = [sd.filter(lambda x: (x[0] == c)).take(1)[0] for c in centroids]
    centroids = sd.flatMap(
lambda x: [(x[0],
                    (c[0], len({**{ k : c[1][k] for k in set(c[1]) - set(x[1]) },
**{ k : x[1][k] for k in set(x[1]) - set(c[1]) }}))
                   ) for c in centroids]
        )\
        .reduceByKey(lambda x, y: min([x, y], key = lambda k: k[1]))\
        .map(lambda x: (x[1][0], [x[0]])).reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0], sorted(x[1])))\
        .flatMap(lambda x: [(x[0], (sdict[state], 1)) for state in x[1]])\
        .reduceByKey(lambda x, y: (
            { k: x[0].get(k, 0) + y[0].get(k, 0) \
for k in set(x[0]) | set(y[0]) }, x[1]+y[1]
        ))\
        .map(lambda x: (x[0], {k: x[1][0][k]/x[1][1] \
for k in x[1][0]}))\
        .collectAsMap()
# The current state is a list of centroids tuples, (identifier, dict)
# A centroid is defined by a dict of flowers as keys, and their presence ratio as a value
    d1 = 0
    d2 = 0
    results = None
while (True):
        distances = sd.flatMap(
lambda x: [(x[0],
                        (ck, sum([(centroids[ck].get(k, 0) - x[1].get(k,0))**2 \
for k in set(centroids[ck]) | set(x[1])]))) for ck in centroids]
            )\
            .reduceByKey(lambda x, y: min([x, y], key = lambda k: k[1]))
        d1 = d2
        d2 = distances.map(lambda x: x[1][1]).reduce(lambda x, y: x + y)
if (pabs(int(d1)-int(d2)) < 1):
            results = distances.map(lambda x: (x[1][0], [x[0]])).reduceByKey(lambda x, y: x + y)\
                    .map(lambda x: (x[0], sorted(x[1])))\
                    .map(lambda x: (x[1][0], x[1][1:]))\
                    .sortByKey(ascending=True)\
                    .map(lambda x: [x[0]]+x[1]).collect()
break
        centroids = distances.map(lambda x: (x[1][0], [x[0]]))\
            .reduceByKey(lambda x, y: x + y)\
            .map(lambda x: (x[0], sorted(x[1])))\
            .flatMap(lambda x: [(x[0], (sdict[state], 1)) for state in x[1]])\
            .reduceByKey(lambda x, y: ({ k: x[0].get(k, 0) + y[0].get(k, 0) \
for k in set(x[0]) | set(y[0]) }, x[1]+y[1] ))\
            .map(lambda x: (x[0], {k: x[1][0][k]/x[1][1] for k in x[1][0]}))\
            .collectAsMap()
return results


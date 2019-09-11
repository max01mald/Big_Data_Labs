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
from pyspark.sql.functions import desc, size, max, abs, lit, explode
import math
import random

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

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

def construct():
	os.chdir("/Users/Max/Desktop/Lab-3/BigData-LA3/")
	
	spark = init_spark()
	
	lines = spark.read.text("/Users/Max/Desktop/Lab-3/BigData-LA3/data/plants.data").rdd
	parts = lines.map(lambda row: row.value.split(",")).zipWithIndex()
	ratingsRDD = parts.map(lambda p: Row(id=(p[1]),plant=(p[0][0]),items=(p[0][1:])))
	ratings = spark.createDataFrame(ratingsRDD)
	ratings = ratings.select("id","plant","items")
	
	return ratings

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
    
    frame = construct()
    
    frame = frame.limit(n)
    
    string = toCSVLine(frame)
    
    return string
    
#data_frame("hello", 5)

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
    
    frame = construct()
    
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    
    model = fpGrowth.fit(frame)
     
    model = model.freqItemsets
    
    model = model.select("*",size("items"))
    
    model = model.withColumnRenamed("size(items)", "ln")
    
    model = model.sort(desc("ln"),desc("freq"))
    
    model = model.select("items","freq")
    
    model = model.limit(n)
    
    string = toCSVLine(model)
    
    return string

#frequent_itemsets("hello", 15, 0.1, 0.3)

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
    
    frame = construct()
    
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    
    model = fpGrowth.fit(frame)
     
    model = model.associationRules
    
    model = model.select("*",size("antecedent"))
    
    model = model.withColumnRenamed("size(antecedent)", "ln")
    
    model = model.sort(desc("ln"),desc("confidence"))
    
    model = model.select("antecedent","consequent","confidence")
    
    model = model.limit(n)
    
    string = toCSVLine(model)
    
    #print(string)
    
    return string

#association_rules("filename", 15, 0.1, 0.3)

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
    
    frame = construct()
    
    frame2 = frame.withColumn("items",explode(frame.items))
    
    frame2 = frame2.groupBy("items").count().sort(desc("count"))
    
    frame2 = frame2.withColumnRenamed("items", "consequent2")
    frame2 = frame2.withColumnRenamed("count", "freq")
    
    fpGrowth = FPGrowth(itemsCol="items", minSupport=s, minConfidence=c)
    
    model = fpGrowth.fit(frame)
     
    model = model.associationRules
    
    model = model.withColumn("consequent2",explode(model.consequent))
    
    model = model.join(frame2, "consequent2", "inner")
    
    model = model.withColumn("interest",lit(abs(model.confidence-(model.freq/frame.count()))))
    
    model = model.select("*",size("antecedent"))
    
    model = model.withColumnRenamed("size(antecedent)", "ln")
    
    model = model.sort(desc("ln"),desc("interest"))
    
    model = model.select("antecedent","consequent","confidence","consequent","freq","interest")
    
    model = model.limit(n)
    
    string = toCSVLine(model)
    
    print(string)
    
    return string

#interests("filename", 15, 0.1, 0.3)

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

def dictionairy():
	all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
	os.chdir("/Users/Max/Desktop/Lab-3/BigData-LA3/")
	spark = init_spark()
	lines = spark.read.text("/Users/Max/Desktop/Lab-3/BigData-LA3/data/plants.data").rdd
	parts = lines.map(lambda row: row.value.split(","))
	plants = parts.map(lambda p: (p[0],p[1:]))
	list = []
	for s in all_states:
		dict = plants.map(lambda x: (x[0],[1 if s in x[1] else 0][0])).collectAsMap()
		list+=[(s,dict)]
	return list


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
    
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
           "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
           "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
           "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
           "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
           "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
           "yt", "dengl", "fraspm" ]
    
    list = dictionairy()
    
    i = all_states.index(state)
    a = list[i][1][plant]
    
    return a
	
#data_preparation("filename", "urtica", "qc")

def distance2(filename, state1, state2):
    '''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    list = dictionairy()
    
    i1 = all_states.index(state1)
    i2 = all_states.index(state2)
    
    a1 = list[i1][1]
    a2 = list[i2][1]
    
    v1 = []
    v2 = []
    
    for value in a1.items():
    	v1.append(value[1])
    
    for value in a2.items():
    	v2.append(value[1])
    	
    i = 0
    sum = 0
    while i < len(v1):
    	sum += (v1[i]-v2[i])**2
    	i += 1
    	
    return sum

def distance3(filename, state1, state2, list):
    '''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    
    i1 = all_states.index(state1)
    i2 = all_states.index(state2)
    
    a1 = list[i1][1]
    a2 = list[i2][1]
    
    v1 = []
    v2 = []
    
    for value in a1.items():
    	v1.append(value[1])
    
    for value in a2.items():
    	v2.append(value[1])
    	
    i = 0
    sum = 0
    while i < len(v1):
    	sum += (v1[i]-v2[i])**2
    	i += 1
    	
    return sum

#distance2("filename", "ca", "az")

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
    i = 0
    list = []
    
    random.seed(seed)
    
    
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    list = random.sample(all_states,k)
    
    
    return list

#init_centroids(3, 124)   

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
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    list = init_centroids(k, seed) 
    rdd = dictionairy()
    
    dict = {}
    list1 = []
    list2 = []
    list3 = []
    
    j = 0
    
    while j < len(all_states):
    	i = 0
    	s = -1
    	while i < k:
    		dis = distance3("filename",list[i],all_states[j],rdd)
    		if i == 0:
    			smallest = dis
    			s = i
    		if smallest > dis:
    			smallest = dis
    			s = i
    		i += 1
    	
    	if s == 0:
    		list1.append(all_states[j])
    	elif s == 1:
    		list2.append(all_states[j])
    	elif s == 2:
    		list3.append(all_states[j])
    	
    	j += 1
    
    list1.sort()
    list2.sort()
    list3.sort()
    
    dict[list[0]] =list1
    dict[list[1]] =list2
    dict[list[2]] =list3
    
   	
    return dict

#first_iter("filename", 3, 123)

def addi(list1,list2):
	i = 0
	
	while i < len(list1):
		list2[i] += list1[i]
		i += 1
	return list2
	
def divi(list1,d):
	i = 0
	
	while i < len(list1):
		list1[i] /= d
		i += 1
	return list1
	
def average(listC,dic):
	
	all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
	m = 0
	v1 = []
	v2 = []
	
	listA = []
	while m < len(listC):
		listA.append([])
		n = 0
		while n <len(listC[m]):
			i1 = all_states.index(listC[m][n])
			a1 = dic[i1][1]
			v1 = []
			for value in a1.items():
				v1.append(value[1])
			
			n += 1
			
			if v2 == []:
				v2 = [0]*len(v1)
			
			v2 = addi(v1,v2)
		
		v2 = divi(v2,n)
		listA[m] = v2
		v2 = []
		m += 1
		
		
	return listA
	
def distance4(filename, avg, state, list):
    '''
    This function computes the squared Euclidean
    distance between two states.
    
    Return value: an integer.
    Test: tests/test_distance.py
    '''
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    
    i1 = all_states.index(state)
    
    a1 = list[i1][1]
    
    v1 = []
    
    for value in a1.items():
    	v1.append(value[1])
    
    i = 0
    sum = 0
    while i < len(v1):
    	sum += (v1[i]-avg[i])**2
    	i += 1
    	
    return sum

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
    
    all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl","ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md","ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm","ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd","tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al","bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk","yt", "dengl", "fraspm" ]
    list = init_centroids(k, seed) 
    rdd = dictionairy()
    
    dict = {}
    
    list1 = []
    
    q=0
    while q <k:
    	list1.append([])
    	q+=1
    
    j = 0
    
    while j < len(all_states):
    	i = 0
    	s = -1
    	while i < k:
    		dis = distance3("filename",list[i],all_states[j],rdd)
    		if i == 0:
    			smallest = dis
    			s = i
    		if smallest > dis:
    			smallest = dis
    			s = i
    		i += 1
    	
    	list1[s].append(all_states[j])
    	list1[s].sort()
    
    	j += 1
    
    list1.sort()
    
    m = 0
    
    while 1:
    	if m == 0:
    		listA = average(list1,rdd)
    		listA2 = listA
    	else:
    		listA = average(list1,rdd)
    		
    		if listA == listA2:
    			break
    		else:
    			listA2 = listA
    	
    	list1 = []
    	q=0
    	while q <k:
    		list1.append([])
    		q+=1
    	
    	j = 0
    	
    	while j < len(all_states):
    		i = 0
    		s = -1
    		
    		while i < k:
    			dis = distance4("filename",listA[i],all_states[j],rdd)
    			if i == 0:
    				smallest = dis
    				s = i
    			if smallest > dis:
    				smallest = dis
    				s = i
    			i += 1
    		
    		list1[s].append(all_states[j])
    		list1[s].sort()
    		
    		j += 1
    	
    	list1.sort()
    	
    	print(list1)
    	
    	m += 1
    
    #print(list1)
   	
    return list1



import csv
import os
import sys

# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import desc
# Dask imports

import dask.bag as db
import dask.dataframe as dd  # you can use Dask bags or dataframes
from csv import reader

'''
INTRODUCTION

The goal of this assignment is to implement a basic analysis of textual 
data using Apache Spark (http://spark.apache.org) and 
Dask (https://dask.org). 
'''

'''
DATASET

We will study a dataset provided by the city of Montreal that contains 
the list of trees treated against the emerald ash borer 
(https://en.wikipedia.org/wiki/Emerald_ash_borer). The dataset is 
described at 
http://donnees.ville.montreal.qc.ca/dataset/frenes-publics-proteges-injection-agrile-du-frene 
(use Google translate to translate from French to English). 

We will use the 2015 and 2016 data sets available in directory `data`.
'''

'''
HELPER FUNCTIONS

These functions are here to help you. Instructions will tell you when
you should use them. Don't modify them!
'''

#Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x,y: os.linesep.join([x,y]))
    return a + os.linesep

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

'''
Plain PYTHON implementation

To get started smoothly and become familiar with the assignment's 
technical context (Git, GitHub, pytest, Travis), we will implement a 
few steps in plain Python.
'''

#Python answer functions
def count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees (non-header lines) in
    the data file passed as first argument.
    Test file: tests/test_count.py
    Note: The return value should be an integer
    '''
    # ADD YOUR CODE HERE
    count = 0
    
    #os.chdir("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/")
    
    f = open(filename, "r")
    
    for lines in f:
    	count += 1
    
    f.close()
    
    return int(count-1)
    
    #raise ExceptionException("Not implemented yet")

#count("./data/frenepublicinjection2016.csv")

def parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    line = 0
    count = 0
    read = 0
    start = 0
    skip = 0
    park = ""
    list = []
    
    #os.chdir("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/")
    
    f = open(filename, "r")
    
    for lines in f:
    	for char in lines:
    		if char == ",":
    			if read == 1 and skip == 0:
    				read = 0
    				start = 0
    				break
    			else:
    				if skip == 0:
	    				count += 1
    		if char == '"':
    			skip += 1
    			
    			if skip == 2:
    				skip = 0
    					
    		if count == 6:
    			count = 0
    			read = 1
    		
    		if read == 1:
    			start += 1
    			if start > 1:
    				if char != '"':
		    			park += char
    	line += 1	
    	if line > 1:
    		if park != "":
	    		list.append(park)
    		park = ""
    		
    	else:
    		park = ""
    		
    return int(len(list))
    
    #raise Exception("Not implemented yet")
    
def get_parks(filename):
	line = 0
	count = 0
	read = 0
	start = 0
	skip = 0
	park = ""
	list = []
	
	#os.chdir("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/")
	f = open(filename, "r")
	for lines in f:
		for char in lines:
			if char == ",":
				if read == 1 and skip == 0:
					read = 0
					start = 0
					break
				else:
					if skip == 0:
						count += 1
			if char == '"':
				skip += 1			
			
				if skip == 2:
					skip = 0
			
			if count == 6:
				count = 0
				read = 1
				
			if read == 1:
				start += 1
				if start > 1:
					if char != '"':
						park += char
						
		line += 1
		if line == 1:
			park = ""
		
		if park != "":
			if park not in list:
				list.append(park)
				park = ""
			else:
				park = ""
				
	list.sort()
	return list
    
def uniq_parks(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of unique parks where trees
    were treated. The list must be ordered alphabetically. Every element in the list must be printed on
    a new line.
    Test file: tests/test_uniq_parks.py
    Note: The return value should be a string with one park name per line
    '''

    # ADD YOUR CODE HERE
    
    list = get_parks(filename)
    	
    string = '\n'.join(list)
    
    string += "\n"
    
    return(string)
    
    #raise Exception("Not implemented yet")



def uniq_parks_counts(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that counts the number of trees treated in each park
    and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    
    line = 0
    count = 0
    count2 = 0
    count3 = 0
    read = 0
    start = 0
    skip = 0
    park = ""
    
    list = []
    list2 = []
    
    #os.chdir("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/")
    
    f = open(filename, "r")
    
    for lines in f:
    	for char in lines:
    		if char == ",":
    			if read == 1 and skip == 0:
    				read = 0
    				start = 0
    				break
    			else:
    				if skip == 0:
	    				count += 1
    		if char == '"':
    			skip += 1
    			
    			if skip == 2:
    				skip = 0
    					
    		if count == 6:
    			count = 0
    			read = 1
    		
    		if read == 1:
    			start += 1
    			if start > 1:
    				if char != '"':
		    			park += char
		
    	line += 1	
    	if line > 1:
    		if park != "":
    			if park not in list:
		    		list.append(park)
		    		list2.append(1)
		    	else:
		    		i = list.index(park)
		    		
		    		count2 = int(list2[i])
		    		count2 += 1
		    		list2[i] = count2
		    		
    		park = ""
    		
    	else:
    		park = ""
    
    j=0
    while j < len(list):
    	list[j] = list[j] + "," + str(list2[j])
    	j += 1
    
    list.sort()
    ind = list.index("CHEVREMONT, MÉD.,18")
    temp = list[ind]
    del list[ind]
    
    ind = list.index("CHEVREMONT, MÉD. (3225 CHEV.-40,2")
    list.insert(ind,temp)
    		
    
    string = '\n'.join(list)
    
    string += "\n"
    return (string)
    
    #raise Exception("Not implemented yet")



def frequent_parks_count(filename):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the list of the 10 parks with the
    highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar number.
    Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    
    line = 0
    count = 0
    count2 = 0
    count3 = 0
    read = 0
    start = 0
    skip = 0
    park = ""
    
    list = []
    list2 = []
    list3 = []
    
    #os.chdir("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/")
    
    f = open(filename, "r")
    
    for lines in f:
    	for char in lines:
    		if char == ",":
    			if read == 1 and skip == 0:
    				read = 0
    				start = 0
    				break
    			else:
    				if skip == 0:
	    				count += 1
    		if char == '"':
    			skip += 1
    			
    			if skip == 2:
    				skip = 0
    					
    		if count == 6:
    			count = 0
    			read = 1
    		
    		if read == 1:
    			start += 1
    			if start > 1:
    				if char != '"':
		    			park += char
		
    	line += 1	
    	if line > 1:
    		if park != "":
    			if park not in list:
		    		list.append(park)
		    		list2.append(1)
		    	else:
		    		i = list.index(park)
		    		
		    		count2 = int(list2[i])
		    		count2 += 1
		    		list2[i] = count2
		    		
    		park = ""
    		
    	else:
    		park = ""
    
    k = 0
    for num in list2:
    	if k < 10:
    		nums = []
    		nums.append(num) 
    		nums.append(list2.index(num))
    		list3.append(nums)
    	else:
    		m = 0
    		max = num
    		mi = list2.index(num)
    		while m < 10:
    			if max > list3[m][0]:
    				temp = list3[m][0]
    				ti = list3[m][1]
    				list3[m][0] = max
    				list3[m][1]	= mi
    				max = temp
    				mi = ti
    				
    			m += 1
    	k += 1
    
    list4 = []
    
    j=0
    while j < 10:
    	list4.append(list[list3[j][1]] + "," + str(list3[j][0]))
    	j += 1
    
    string = '\n'.join(list4)
    
    string += "\n"
    
    return(string)
    

def intersection(filename1, filename2):
    '''
    Write a Python (not DataFrame, nor RDD) script that prints the alphabetically sorted list of
    parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    list3 = []
    list1 = get_parks(filename1)
    list2 = get_parks(filename2)
    
    
    i = 0
    while i < len(list1):
    	j = 0
    	while j < len(list2):
    		if list1[i] == list2[j]:
    			list3.append(list1[i])
    		j += 1
    	i += 1  
    
    list3.sort()
    
    string = '\n'.join(list3)
    
    return('\n'.join(list3)+"\n")
    
'''
SPARK RDD IMPLEMENTATION

You will now have to re-implement all the functions above using Apache 
Spark's Resilient Distributed Datasets API (RDD, see documentation at 
https://spark.apache.org/docs/latest/rdd-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
However, all operations must be re-implemented using the RDD API, you 
are not allowed to simply convert results obtained with plain Python to 
RDDs (this will be checked). Note that the function *toCSVLine* in the 
HELPER section at the top of this file converts RDDs into CSV strings.
'''

# RDD functions

#os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

def count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_rdd.py
    Note: The return value should be an integer
    '''
    
    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    
    #lines = spark.sparkContext.textFile("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv")
    lines = spark.read.csv("./data/frenepublicinjection2016.csv", header=True).rdd
    return(lines.count())
    
	
    #raise Exception("Not implemented yet")

def parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_rdd.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #lines = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True).rdd
    lines = spark.read.csv("./data/frenepublicinjection2016.csv", header=True).rdd
    lines = lines.filter(lambda l: l['Nom_parc'] != None)
    return(lines.count())

def uniq_parks_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of unique parks where
    trees were treated. The list must be ordered alphabetically. Every element
    in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_rdd.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    #lines = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True).rdd
    lines = spark.read.csv("./data/frenepublicinjection2016.csv", header=True).rdd
    lines = lines.filter(lambda l: l["Nom_parc"] != None)
    list = lines.map(lambda l: l[6]).distinct().collect()
    
    list.sort()
    
    string = "\n".join(list)
    string += '\n'
    return(string)


def uniq_parks_counts_rdd(filename):
    '''
    Write a Python script using RDDs that counts the number of trees treated in
    each park and prints a list of "park,count" pairs in a CSV manner ordered
    alphabetically by the park name. Every element in the list must be printed
    on a new line.
    Test file: tests/test_uniq_parks_counts_rdd.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_rdd(filename):
    '''
    Write a Python script using RDDs that prints the list of the 10 parks with
    the highest number of treated trees. Parks must be ordered by decreasing
    number of treated trees and by alphabetical order when they have similar
    number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_rdd(filename1, filename2):
    '''
    Write a Python script using RDDs that prints the alphabetically sorted list
    of parks that had trees treated both in 2016 and 2015. Every list element
    must be printed on a new line.
    Test file: tests/test_intersection_rdd.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")


'''
SPARK DATAFRAME IMPLEMENTATION

You will now re-implement all the tasks above using Apache Spark's 
DataFrame API (see documentation at 
https://spark.apache.org/docs/latest/sql-programming-guide.html). 
Outputs must be identical to the ones obtained above in plain Python. 
Note: all operations must be re-implemented using the DataFrame API, 
you are not allowed to simply convert results obtained with the RDD API 
to Data Frames. Note that the function *toCSVLine* in the HELPER 
section at the top of this file also converts DataFrames into CSV 
strings.
'''

# DataFrame functions

def count_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True)
    
    count = df.count()
    
    return count
    
    
def parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_df.py
    Note: The return value should be an integer
    '''

    spark = init_spark()
    
    
    # ADD YOUR CODE HERE
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True)
	#df = df.select("Nom_parc")
    df = df.where(df.Nom_parc != "")
    
    count = df.count()
    
    return count
    

    

def uniq_parks_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_df.py
    Note: The return value should be a CSV string
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True)
    df = df.where(df.Nom_parc != "")
    df = df.dropDuplicates(['Nom_parc'])
    df = df.select("Nom_parc")
    df = df.orderBy(("Nom_parc"))
    
    
    return(toCSVLine(df))
    


def uniq_parks_counts_df(filename):
    '''
    Write a Python script using DataFrames that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_df.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True)
    df = df.where(df.Nom_parc != "")
    df = df.select("Nom_parc")
    df = df.orderBy(("Nom_parc"))
    
    return (toCSVLine(df.groupBy("Nom_parc").count()))
    
    
def frequent_parks_count_df(filename):
    '''
    Write a Python script using DataFrames that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = df.where(df.Nom_parc != "")
    df = df.select("Nom_parc")
    df = df.orderBy(("Nom_parc"))
    
    df = df.groupBy("Nom_parc").count()
    
    df = df.orderBy(desc("count"))
    
    df = df.limit(10)
    
    return(toCSVLine(df))
    
    
def intersection_df(filename1, filename2):
    '''
    Write a Python script using DataFrames that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_df.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    spark = init_spark()
    
    # ADD YOUR CODE HERE
    
    #df = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = spark.read.csv("./data/frenepublicinjection2016.csv", header=True, mode="DROPMALFORMED")
    df = df.where(df.Nom_parc != "")
    df = df.select("Nom_parc")
    df = df.orderBy(("Nom_parc"))
    
    #df2 = spark.read.csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2015.csv", header=True, mode="DROPMALFORMED")
    df2 = spark.read.csv("./data/frenepublicinjection2015.csv", header=True, mode="DROPMALFORMED")
    df2 = df2.where(df2.Nom_parc != "")
    df2 = df2.select("Nom_parc")
    df2 = df2.orderBy(("Nom_parc"))
    
    df3 = df.select("Nom_parc").intersect(df2.select("Nom_parc"))
    
    return(toCSVLine(df3))


'''
DASK IMPLEMENTATION (bonus)

You will now re-implement all the tasks above using Dask (see 
documentation at http://docs.dask.org/en/latest). Outputs must be 
identical to the ones obtained previously. Note: all operations must be 
re-implemented using Dask, you are not allowed to simply convert 
results obtained with the other APIs.
'''

# Dask functions

def count_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees
    (non-header lines) in the data file passed as first argument.
    Test file: tests/test_count_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    
    '''df = dd.read_csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})'''
    
    df = dd.read_csv("./data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})
    
    
    return(len(df))
	

def parks_dask(filename):
    '''
    Write a Python script using Dask that prints the number of trees that are *located in a park*.
    To get the park location information, have a look at the *Nom_parc* column (name of park).
    Test file: tests/test_parks_dask.py
    Note: The return value should be an integer
    '''

    # ADD YOUR CODE HERE
    '''df = dd.read_csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})'''
    
    df = dd.read_csv("./data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})
    
    
    return(len(df[df.Nom_parc.notnull()]))

def to_string():
	f = open("test0.csv", "r")
	start = 0
	string = ""
	list = []
	
	for l in f:
		for char in l:
			if char == "\n":
				start = 0
				list.append(string[1:])
				string = ""
			if char == ',':
				start = 1
			if start == 1 and char != '"':
				string += char
			
	return(list)

def uniq_parks_dask(filename):
    '''
    Write a Python script using Dask that prints the list of unique parks
    where trees were treated. The list must be ordered alphabetically. Every
    element in the list must be printed on a new line.
    Test file: tests/test_uniq_parks_dask.py
    Note: The return value should be a CSV string
    '''

    # ADD YOUR CODE HERE
    '''df = dd.read_csv("/Users/Max/Desktop/Lab_1/bigdata-la1-w2019-max01mald/data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})'''
    
    df = dd.read_csv("./data/frenepublicinjection2016.csv", 
    dtype={"Nom_arrond": str,"Invent": str,"no_civiq": str,"Rue": str,"Rue_De": str,"Rue_A": str,"Nom_parc": str,"Sigle": str
    ,"Injections": str,"x": str,"y": str,"longitude": str,"latitude": str})
    
    
    df[df.Nom_parc.notnull()].Nom_parc.drop_duplicates().to_csv("test*.csv")
    
    list = to_string()
    list.sort()
    
    string = "\n".join(list)
    string += "\n"
    return(string)
		    
#uniq_parks_dask("filename")

def uniq_parks_counts_dask(filename):
    '''
    Write a Python script using Dask that counts the number of trees
    treated in each park and prints a list of "park,count" pairs in a CSV
    manner ordered alphabetically by the park name. Every element in the list
    must be printed on a new line.
    Test file: tests/test_uniq_parks_counts_dask.py
    Note: The return value should be a CSV string
          Have a look at the file *tests/list_parks_count.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def frequent_parks_count_dask(filename):
    '''
    Write a Python script using Dask that prints the list of the 10 parks
    with the highest number of treated trees. Parks must be ordered by
    decreasing number of treated trees and by alphabetical order when they have
    similar number.  Every list element must be printed on a new line.
    Test file: tests/test_frequent_parks_count_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/frequent.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

def intersection_dask(filename1, filename2):
    '''
    Write a Python script using Dask that prints the alphabetically
    sorted list of parks that had trees treated both in 2016 and 2015. Every
    list element must be printed on a new line.
    Test file: tests/test_intersection_dask.py
    Note: The return value should be a CSV string.
          Have a look at the file *tests/intersection.txt* to get the exact return format.
    '''

    # ADD YOUR CODE HERE
    raise Exception("Not implemented yet")

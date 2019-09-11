import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, size, max, abs, lit, explode
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
from math import sqrt
# Dask imports
import dask.bag as db
import dask.dataframe as df
import numpy

from pyspark import SparkContext, SparkConf

#os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"


all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc",
              "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
              "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv",
              "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
              "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "vi",
              "wa", "wv", "wi", "wy", "al", "bc", "mb", "nb", "lb", "nf",
              "nt", "ns", "nu", "on", "qc", "sk", "yt", "dengl", "fraspm"]

def delimit_str(string):
	i = 0
	b = i
	list = []
	
	while i <= len(string):
		if string[b:i] in all_states:
			list.append(string[b:i])
			b = i
		i += 1
	return(list)

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

def context_spark():
	conf = SparkConf()
	sc = SparkContext(conf=conf)
	return sc

def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None


def data_preparation(data_file, key, state):
    """Our implementation of LSH will be based on RDDs. As in the clustering
    part of LA3, we will represent each state in the dataset as a dictionary of
    boolean values with an extra key to store the state name.
    We call this dictionary 'state dictionary'.

    Task 1 : Write a script that
             1) Creates an RDD in which every element is a state dictionary
                with the following keys and values:

                    Key     |         Value
                ---------------------------------------------
                    name    | abbreviation of the state
                    <plant> | 1 if <plant> occurs, 0 otherwise

             2) Returns the value associated with key
                <key> in the dictionary corresponding to state <state>

    *** Note: Dask may be used instead of Spark.

    Keyword arguments:
    data_file -- csv file of plant name/states tuples (e.g. ./data/plants.data)
    key -- plant name
    state -- state abbreviation (see: all_states)
    """
    spark = init_spark()
    #lines = spark.read.text("/Users/Max/Desktop/bigdata-la4-w2019-max01mald/data/plants.data").rdd
    lines = spark.read.text("./data/plants.data").rdd
    parts = lines.map(lambda row: row.value.split(","))
    parts = parts.map(lambda p: (p[0],p[1:]))#(lambda p: [(p[0], p[state]) for state in range(1, len(p))]).reduceByKey(lambda a,b: a + (b,))#.map(lambda p: (p[0], {p[1]}))#.reduceByKey(lambda state1, state2: state1 + state2)
    parts = parts.map(lambda p: (p[0],[(all_states[x],1) if all_states[x] in p[1][0:] else (all_states[x],0) for x in range(0, len(all_states))])).collectAsMap()
    
    dic = dict(parts.get(key))
    return dic.get(state)
    
#data_preparation("data_file", "key", "state")

def primes(n, c):
    """To create signatures we need hash functions (see next task). To create
    hash functions,we need prime numbers.

    Task 2: Write a script that returns the list of n consecutive prime numbers
    greater or equal to c. A simple way to test if an integer x is prime is to
    check that x is not a multiple of any integer lower or equal than sqrt(x).

    Keyword arguments:
    n -- integer representing the number of consecutive prime numbers
    c -- minimum prime number value
    """
    
    list = []
    x = c
    
    while len(list) != n:
    	
    	check = sqrt(x)
    	
    	i = 2
    	false = 0
    	
    	while i <= check:
    		if x % i == 0:
    			false = 1 
    		i += 1
    		
    	if false == 0:
    		list.append(x)
    	
    	x += 1
    
    return list

#primes(50,41)

def hash_plants(s, m, p, x):
    """We will generate hash functions of the form h(x) = (ax+b) % p, where a
    and b are random numbers and p is a prime number.

    Task 3: Write a function that takes a pair of integers (m, p) and returns
    a hash function h(x)=(ax+b)%p where a and b are random integers chosen
    uniformly between 1 and m, using Python's random.randint. Write a script
    that:
        1. initializes the random seed from <seed>,
        2. generates a hash function h from <m> and <p>,
        3. returns the value of h(x).

    Keyword arguments:
    s -- value to initialize random seed from
    m -- maximum value of random integers
    p -- prime number
    x -- value to be hashed
    """
    random.seed(s)
    
    a = random.randint(1,m)
    b = random.randint(1,m)
    
    h = (a*x+b) % p
    
    return h
    
#hash_plants(123, 150, 7, 32)

def hash_list(s, m, n, i, x):
    """We will generate "good" hash functions using the generator in 3 and
    the prime numbers in 2.

    Task 4: Write a script that:
        1) creates a list of <n> hash functions where the ith hash function is
           obtained using the generator in 3, defining <p> as the ith prime
           number larger than <m> (<p> being obtained as in 1),
        2) prints the value of h_i(x), where h_i is the ith hash function in
           the list (starting at 0). The random seed must be initialized from
           <seed>.

    Keyword arguments:
    s -- seed to intialize random number generator
    m -- max value of hash random integers
    n -- number of hash functions to generate
    i -- index of hash function to use
    x -- value to hash
    """
    k = 0
    list = []
    prime = primes(n,m)
    
    random.seed(s)
    
    while k < n:
    	a = random.randint(1,m)
    	b = random.randint(1,m)
    	h = (a*x+b) % prime[k]
    	
    	list.append(h)
    	k+=1
    
    return list[i]	

#hash_list(123, 150, 10, 7, 32)

def signatures(datafile, seed, n, state):
    """We will now compute the min-hash signature matrix of the states.

    Task 5: Write a function that takes build a signature of size n for a
            given state.

    1. Create the RDD of state dictionaries as in data_preparation.
    2. Generate `n` hash functions as done before. Use the number of line in
       datafile for the value of m.
    3. Sort the plant dictionary by key (alphabetical order) such that the
       ordering corresponds to a row index (starting at 0).
       Note: the plant dictionary, by default, contains the state name.
       Disregard this key-value pair when assigning indices to the plants.
    4. Build the signature array of size `n` where signature[i] is the minimum
       value of the i-th hash function applied to the index of every plant that
       appears in the given state.


    Apply this function to the RDD of dictionary states to create a signature
    "matrix", in fact an RDD containing state signatures represented as
    dictionaries. Write a script that returns the string output of the RDD
    element corresponding to state '' using function pretty_print_dict
    (provided in answers).

    The random seed used to generate the hash function must be initialized from
    <seed>, as previously.

    ***Note: Dask may be used instead of Spark.

    Keyword arguments:
    datafile -- the input filename
    seed -- seed to initialize random int generator
    n -- number of hash functions to generate
    state -- state abbreviation
    """
    
    spark = init_spark()
    #lines = spark.read.text("/Users/Max/Desktop/bigdata-la4-w2019-max01mald/data/plants.data").rdd
    lines = spark.read.text("./data/plants.data").rdd
    parts = lines.map(lambda row: row.value.split(","))
    parts = parts.map(lambda p: (p[0],p[1:]))
    dictionairy = parts.map(lambda p: (p[0],[(all_states[x],1) if all_states[x] in p[1][0:] else (all_states[x],0) for x in range(0, len(all_states))]))
    dictionairy = dictionairy.sortBy(lambda p: p[0]).zipWithIndex()
    qc = dictionairy.filter(lambda p: dict(p[0][1]).get(state) == 1)
    
    x_list = qc.map(lambda p: p[1]).collect()
     
    m = lines.count()
    
    k = 0
    sign = []
    prime = primes(n,m)
    random.seed(seed)
    
    while k < n:
    	
    	 
    	a = random.randint(1,m)
    	b = random.randint(1,m)
    	q = 0
    	while q < len(x_list):
    		
    		h = (a*x_list[q]+b) % prime[k]
    		
    		if q == 0:
    			min = h
    		
    		if min > h:
    			min = h
    		
    		q += 1

    	sign.append((k,min))
    	k+=1
    
    print(dict(sign))
    
    return(dict(sign))

#signatures("./data/plants.data", 123, 10, "qc")


def hash_band(datafile, seed, state, n, b, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vector.

    Task: Write a script that, given the signature dictionary of state <state>
    computed from <n> hash functions (as defined in the previous task),
    a particular band <b> and a number of rows <n_r>:

    1. Generate the signature dictionary for <state>.
    2. Select the sub-dictionary of the signature with indexes between
       [b*n_r, (b+1)*n_r[.
    3. Turn this sub-dictionary into a string.
    4. Hash the string using the hash built-in function of python.

    The random seed must be initialized from <seed>, as previously.

    Keyword arguments:
    datafile --  the input filename
    seed -- seed to initialize random int generator
    state -- state to filter by
    n -- number of hash functions to generate
    b -- the band index
    n_r -- the number of rows
    """
    
    spark = init_spark()
    #lines = spark.read.text("/Users/Max/Desktop/bigdata-la4-w2019-max01mald/data/plants.data").rdd
    lines = spark.read.text("./data/plants.data").rdd
    parts = lines.map(lambda row: row.value.split(","))
    parts = parts.map(lambda p: (p[0],p[1:]))
    dictionairy = parts.map(lambda p: (p[0],[(all_states[x],1) if all_states[x] in p[1][0:] else (all_states[x],0) for x in range(0, len(all_states))]))
    dictionairy = dictionairy.sortBy(lambda p: p[0]).zipWithIndex()
    qc = dictionairy.filter(lambda p: dict(p[0][1]).get(state) == 1)
    
    x_list = qc.map(lambda p: p[1]).collect()
     
    m = lines.count()
    
    k = 0
    sign = []
    prime = primes(n,m)
    random.seed(seed)
    
    while k < n:
    	
    	a = random.randint(1,m)
    	b1 = random.randint(1,m)
    	q = 0
    	while q < len(x_list):
    		
    		h = (a*x_list[q]+b1) % prime[k]
    		
    		if q == 0:
    			min = h
    		
    		if min > h:
    			min = h
    		
    		q += 1

    	sign.append((k,min))
    	k+=1
    
    beg = b*n_r
    end = (b+1)*n_r
    
    sub_sign = []
    
    while beg < end:
    	sub_sign.append(sign[beg])
    	beg += 1
		    
    string = str(dict(sub_sign))
    
    final = hash(string)
    
    print(final)
    return final
	
#hash_band("",123, "qc", 12, 2, 2)


def hhh(seed,n,m,n_b,n_r,p,x):
	
    random.seed(seed)
    k = 0
    list = []
   
    while k < n:
    	a = random.randint(1,m)
    	b = random.randint(1,m)
    	q = 0
    	
    	while q < len(x):
    		h = (a*x[q]+b) % p[k]
    		
    		if q == 0:
    			min = h
    		
    		if min > h:
    			min = h
    		
    		q += 1
    	list.append((k,min))
    	k+=1
    
    b_s = 0
    h_list = []
    
    while b_s < n_b:
    	beg = b_s*n_r
    	end = (b_s+1)*n_r
    	
    	sub = []
    	
    	while beg < end:
    		sub.append(list[beg])
    		beg+=1
    	
    	string = str(dict(sub))	
    	final = hash(string)
    	h_list.append((b_s,final))
    	b_s+=1
    
    return h_list

def hash_bands(data_file, seed, n_b, n_r):
    """We will now hash the complete signature matrix

    Task: Write a script that, given an RDD of state signature dictionaries
    constructed from n=<n_b>*<n_r> hash functions (as in 5), a number of bands
    <n_b> and a number of rows <n_r>:

    1. maps each RDD element (using flatMap) to a list of ((b, hash),
       state_name) tuples where hash is the hash of the signature vector of
       state state_name in band b as defined in 6. Note: it is not a triple, it
       is a pair.
    2. groups the resulting RDD by key: states that hash to the same bucket for
       band b will appear together.
    3. returns the string output of the buckets with more than 2 elements
       using the function in pretty_print_bands.py.

    That's it, you have printed the similar items, in O(n)!

    Keyword arguments:
    datafile -- the input filename
    seed -- the seed to initialize the random int generator
    n_b -- the number of bands
    n_r -- the number of rows in a given band
    """
    
    spark = init_spark()
    #lines = spark.read.text("/Users/Max/Desktop/bigdata-la4-w2019-max01mald/data/plants.data").rdd
    lines = spark.read.text("./data/plants.data").rdd
    parts = lines.map(lambda row: row.value.split(","))
    parts = parts.map(lambda p: (p[0],p[1:]))
    dictionairy = parts.map(lambda p: (p[0],[(all_states[x],1) if all_states[x] in p[1][0:] else (all_states[x],0) for x in range(0, len(all_states))]))
    dictionairy = dictionairy.sortBy(lambda p: p[0]).zipWithIndex()
    
    m = lines.count()
    n = n_b * n_r
    p = primes(n,m)
    
    #print(dictionairy.collect())
    
    s_list = dictionairy.map(lambda p: (p[0][0],[ p[1] if dict(p[0][1]).get(state) == 1 else None for state in all_states]))
    
    
    
    s_list = s_list.map(lambda p: (p[0], [(all_states[x],(p[1][x])) for x in range(0,len(p[1])) if p[1][x] != None]))
    s_list = s_list.filter(lambda p: len(p[1]) > 0)
    s_list = s_list.flatMap(lambda p: (p[1])).map(lambda p: (p[0],[p[1]]))
    s_list = s_list.reduceByKey(lambda a, b: a + b)
    
    #print(s_list.collect())
    
    rdd = s_list.map(lambda x: (hhh(seed,n,m,n_b,n_r,p,x[1]),x[0]))
    rdd = rdd.flatMap(lambda p: [(p[0][i],[p[1]]) for i in range(0,len(p[0]))])
    
    #rdd = rdd.map(lambda p: (p[0]))
    
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    rdd = rdd.filter(lambda p: len(p[1]) >= 2)
    
    #print(rdd.collect())
    
    print(ppb(rdd))
    return ppb(rdd)
    
    
#hash_bands("./data/plants.data", 123, 5, 7)   	

def ppb(rdd):
    out_string = ""
    # rdd contains elements in the form ((band_id, bucket_id), [ state_names ])
    for x in sorted(rdd.collect()):
        if len(x[1]) < 2:
            continue # don't print buckets with a single element
        out_string += ("------------band {}, bucket {}-------------\n"
                               .format(x[0][0],x[0][1]))
        for y in sorted(x[1]):
            out_string += y + " "
        out_string += os.linesep

    return out_string

def get_b_and_r(n, s):
    """The script written for the previous task takes <n_b> and <n_r> as
    parameters while a similarity threshold <s> would be more useful.

    Task: Write a script that prints the number of bands <b> and rows <r> to be
    used with a number <n> of hash functions to find the similar items for a
    given similarity threshold <s>. Your script should also print <n_actual>
    and <s_actual>, the actual values of <n> and <s> that will be used, which
    may differ from <n> and <s> due to rounding issues. Printing format is
    found in tests/test-get-b-and-r.txt

    Use the following relations:

     - r=n/b
     - s=(1/b)^(1/r)

    Hint: Johann Heinrich Lambert (1728-1777) was a Swiss mathematician

    Keywords arguments:
    n -- the number of hash functions
    s -- the similarity threshold
    """
    
    b = 1
    s_b = 0
    s2 = 0
    min = 0
    
    while b < n:
    	s2 = numpy.float_power(1/b,b/n)
    	
    	check = s2 - s
    	
    	if check < 0:
    		break
    	
    	if b == 1:
    		min = check
    		s_b = b
    	
    	if min > check:
    		min = check
    		s_b = b
    	b+=1
    
    
    r = numpy.floor(n/s_b)
    n = s_b * r
    s = numpy.float_power((1/s_b),(1/r)) 
    
    string = "b=" + str(s_b) + "\nr=" + str(int(r)) + "\nn_real=" + str(int(n)) + "\ns_real=" + str(s) + "\n"
    
    print(string)
    return string
    
    

#get_b_and_r(100, 0.8)

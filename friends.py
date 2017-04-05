from pyspark import SparkContext
from math import log

# TODO: Add input features.
# TODO: Add docstring.

'''
Functions
'''

def log2(x):
    return log(x)/log(2)
'''
Main
'''

def main():
    # Instance of Spark
    sc = SparkContext("local", "Simple App")

    # Filename
    graphFile = "data/test/graph1.txt" 

    # Base RDD or the graph
    graph = sc.textFile(graphFile).map(lambda line: map(int, line.split()))

    # Distinct elements
    distict = graph.flatMap(lambda line: map(int, line.split())).distinct()

    # Another way
    distinct = sc.parallelize(range(1, graph.map(lambda line: line).count()+1)).collect()

    # Get all the connected edges
    c_edges = graph.flatMap(lambda state: [(i, j) if i < j else (j, i) for i in [state[0]] for j in state[1:]]).collect()

    # Get all the possible new edges

    # notConnected = graph.map(lambda state: [(state[0], j) if state[0]<j else (j, state[0]) for i in state[1:] for j in [x for x in distinct if x not in state]]).flatMap(lambda x: x)

    notConnected = graph.map(lambda state: [((min(state[0], j), max(state[0], j)), (min(i, j), max(i, j)))  for i in state[1:] for j in [x for x in distinct if x not in state]]).flatMap(lambda x: x).filter(lambda edge: edge[1] in c_edges).map(lambda edge: (edge[0], 2))

    # Remove from not connected 

    connected = graph.map(lambda state: [((i, j), 0) if i<j else ((j, i), 0) for i in [state[0]] for j in state[1:]]).flatMap(lambda x: x)

    # notConnected = graph.map(lambda state: [((state[0], j), 2) if state[0]<j else ((j, state[0]), 2) for i in state[1:] for j in [x for x in distinct if x not in state]])

    recommend = connected.union(notConnected).reduceByKey(lambda a,b: a*b)

    print(recommend.takeOrdered(10, key = lambda x: -x[1])) 

    sc.stop()

if __name__ == "__main__":
    main()

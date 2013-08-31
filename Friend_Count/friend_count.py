import MapReduce
import sys

"""
Consider a simple social network dataset consisting of key-value pairs
where each key is a person and each value is a friend of that person.
Describe a MapReduce algorithm to count he number of friends each person has.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # The input is a 2 element list: [personA, personB]
  
    personA = record[0]
    personB = record[1]
    mr.emit_intermediate(personA, 1 )

def reducer(key, list_of_values):
    '''
    The output should be a (person,  friend count) tuple
    '''
    total = 0
    for e in list_of_values:
      total += e
    mr.emit((key, total))  

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

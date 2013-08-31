import MapReduce
import sys

"""
The relationship "friend" is often symmetric, meaning that if I am your friend,
you are my friend. Implement a MapReduce algorithm to check whether this
property holds. Generate a list of all non-symmetric friend relationships.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    '''
    The input is a 2 element list: [personA, personB]
    personA: Name of a person formatted as a string
    personB: Name of one of personAs friends formatted as a string
    This implies that personB is a friend of personA, but it does not imply
    that personA is a friend of personB.
    '''
    friends_pair = tuple(sorted(record))
    mr.emit_intermediate(friends_pair , 1)

def reducer(key, list_of_values):
    '''
    The output should be a list of (person, friend) and
    (friend, person) tuples for each asymmetric friendship
    Only one of the (person, friend) or (friend, person) output tuples
    will exist in the input. This indicates friendship asymmetry.
    '''        
    if len(list_of_values) == 1:    
        mr.emit( (key[0], key[1]) )
        mr.emit( (key[1], key[0]) ) 

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

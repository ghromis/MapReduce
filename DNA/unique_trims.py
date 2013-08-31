import MapReduce
import sys

"""
Consider a set of key-value pairs where each key is sequence id and
each value is a string of nucleotides, e.g., GCTTCCGAAATGCTCGAA....

Write a MapReduce query to remove the last 10 characters from each string
of nucleotides, then remove any duplicates generated.
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    '''
    The input is a 2 element list: [sequence id, nucleotides]
    sequence id: Unique identifier formatted as a string
    nucleotides: Sequence of nucleotides formatted as a string
    '''
    nucleotides = record[1]
    trimmed = nucleotides[:-10]
    mr.emit_intermediate(trimmed, 1)

def reducer(key, list_of_values):
    '''
    The output from the reduce function should be the unique
    trimmed nucleotide strings.
    '''

    mr.emit(key)

        
# Do not modify below this line
# =============================

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

import MapReduce
import sys

"""
Assume you have two matrices A and B in a sparse matrix format,
where each record is of the form i, j, value.
Design a MapReduce algorithm to compute matrix multiplication: A x B
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

N = 5

def mapper(record):
    '''
    The input to the map function will be matrix row records formatted as lists.
    Each list will have the format [matrix, i, j, value] where matrix is a
    string and i, j, and value are integers.
    The first item, matrix, is a string that identifies which matrix
    the record originates from. This field has two possible values:
    'a' indicates that the record is from matrix A
    'b' indicates that the record is from matrix B
    '''
    matrix, row, col, value = record   
    if matrix == 'a':
        for n in range(N):
            k = record[1]
            mr.emit_intermediate((k, n),[matrix, col, value])
    else:
        for n in range(N):
            k = record[2]
            mr.emit_intermediate((n, k),[matrix, row, value])
 

def reducer(key, list_of_values):
    '''
    The output from the reduce function will also be matrix row records
    formatted as tuples. Each tuple will have the format (i, j, value)
    where each element is an integer.
    '''
    a_value= [e for e in list_of_values if e[0] == 'a' ]
    b_value = [e for e in list_of_values if e[0] == 'b']
    result = 0
    for a in a_value:
        for b in b_value:
            if a[1] == b[1] :
                result += a[2] * b[2]
    mr.emit( ( key[0], key[1], result ) )

        
# Do not modify below this line
# =============================

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

import MapReduce
import sys

"""
Implement a relational join as a MapReduce query

Consider the query:


SELECT * 

FROM Orders, LineItem 

WHERE Order.order_id = LineItem.order_id


Your MapReduce query should produce the same information as this SQL query.
You can consider the two input tables, Order and LineItem, as one big concatenated
bag of records which gets fed into the map function record by record.  
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

TABLE_1_NAME = 'order'
TABLE_2_NAME = 'line_item'


def mapper(record):
    '''
    The input will be database records formatted as lists of Strings.
    The first item(index 0) in each record is a string that identifies which table
    the record originates from. This field has two possible values:
        line_item indicates that the record is a line item.
        order indicates that the record is an order.
    The second element(index 1) in each record is the order_id.
    '''
    table_name = record[0]
    order_id = record[1]
    product_description = record[2:]
    
    mr.emit_intermediate( order_id, [ table_name, product_description] )

def reducer(key, list_of_values):
    '''
    The output should be a joined record.
    '''
    table1 = [table[1] for table in list_of_values if table[0] == TABLE_1_NAME ]
    table2 = [table[1] for table in list_of_values if table[0] == TABLE_2_NAME ]

    for record1 in table1:
        for record2 in table2:
            joined = []

            joined.append(TABLE_1_NAME)
            joined.append(key)
            joined.extend(record1)
            joined.append(TABLE_2_NAME)
            joined.append(key)
            joined.extend(record2)           

            mr.emit( joined )

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

# This file is used to insert the generated file from getDataDistribution.py
# to the database.
#
# Notice: guest database account will not have right to modify the database.

import psycopg2
import sys
import re
import scoresentence

START = 1 # start file name
END = 2   # end file name

if __name__ == "__main__":
    ret = scoresentence.dbConnect()
    conn = ret[1]
    cursor = ret[0]
    for x in range(START, END): 
        count = 0
        f = open("scoredistributiontop" + str(x) + "000.txt", 'r')
        for line in f:
            count = count + 1
            split = line.strip().split(", ")
            line = "INSERT INTO features_test values('" + split[0] + "', '" + split[1] + "', " + split[2] + ", " + split[3] + ", " + split[4] + ", " + split[5] + ", " + split[6] + "); "
            print "Insert into the row " + str(count)
            cursor.execute(line)
    conn.commit()
    cursor.close()
    conn.close()

# This function will calculate the average star of all the words showing
# in the review data. Result will be printed to a file or insert to 
# database.
#
# Notice: guest database account will not have right to modify the database.

import nltk
from nltk.stem.lancaster import LancasterStemmer
import psycopg2
import sys
import pprint
import re
import scoresentence

CUTOFF = 3    # Will not consider the word with frequency less than 3
TOFILE = True # True if print to file; False to insert to database.

# Fecth review data from the database to calculate the average star of each word
def parseword(cursor):
    cursor.execute("SELECT content, stars FROM yelp_review_flat limit 1000;")
    records = cursor.fetchall()
    st = LancasterStemmer()
  
    word = []    # Record the word
    count = []   # Record the frequency of the word
    starSum = [] # Record the total star of the word
    for x in range(0, len(records)):
        content = records[x][0]
        content = content.lower()
        star = records[x][1]
        content = re.sub('n\'t', 'n', content)
        content = re.sub('[^0-9a-zA_Z]+',' ', content)
        split_content = nltk.word_tokenize(content)
        print "Running records... "+ str(x+1)
        for y in range(0, len(split_content)):
            item = split_content[y]
            if (item not in word):
                word.append(item)
                count.append(1)
                starSum.append(star)
            else:
                index = word.index(item)
                count[index] = count[index] + 1
                starSum[index] = starSum[index] + star

    stat = open("result.txt", "w")
    total_count = 0
    total_star = 0
    for t in range(0, len(word)):
        print "Produce records " + str(t + 1) + " in " + str(len(word))
        if (count[t] >= CUTOFF):
            total_count = total_count + count[t]
            total_star = total_star + starSum[t]
            out = word[t] + ", " + str(count[t]) + ", " + str(starSum[t])
            if (TOFILE):
                print >>stat, out
            else:
                cursor.execute("INSERT INTO word_star (word, number, star_sum) values(%s, %s, %s);",(word[t], count[t], starSum[t]))

    total_ave = str(total_star / total_count)
    print total_ave

if __name__ == "__main__":
  connect = scoresentence.dbConnect()
  conn = connect[1]
  cursor = connect[0]
  parseword(cursor)
  conn.commit()
  cursor.close()
  conn.close()
  


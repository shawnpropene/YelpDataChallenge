# This function will generate the score of a given sentence 
# based on the word score generated by wordstar.py
# Instead of inserting the reuslt in to database, the result 
# is printed in terminal.
#
# Notice: guest database account will not have right to modify the database.

import nltk
import psycopg2
import sys
from nltk.stem.lancaster import LancasterStemmer
import re

# Define the useful NLTK tags for later word filtering. 
tags = ["FW", "JJ", "JJR", "JJS", "NN", "NNP", "NNPS", "NNS", "RB", "RBR", "RBS", "UH", "VB", "VBD", "VBG", "VBN", "VBP", "VBZ"]

# This function is used to connect to the database.
def dbConnect():
    conn_string = "host='abstract.cs.washington.edu' port='5252' dbname='yelp' user='db_guest' password='guest123'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print "Succesfully Connected to database! \n"
    return (cursor, conn)

# This function print the instruction.
def printUsage():
    print "Please enter the setence(s) you want us to score. "
    print "One per line. Press Ctrl+D to finish. "
    print "==================================================================\n"

# Score a set of given sentence.
def scoreSentence(cursor, page, st, avg_score, std_dev):
    avg_score = float(avg_score)

    # Returned fields
    score = []
    weighted_score = []
    maximum = []
    minimum = []
    count = []

    temp = []
    for line in page:
        temp.append(line[0:-1]) #move away the new line character if using standard input.
    page = temp

    for line in page:
        ret = processline(cursor, line, st, avg_score, std_dev)
        count.append(ret[0])

        if (ret[0] > 0):                  #If count > 0
            score.append(ret[1] / ret[0]) #score_sum / count
            maximum.append(ret[2])        #score_max
            minimum.append(ret[3])        #score_min
            weighted_score.append(ret[4]) #weighted_sum

        else:
            score.append(avg_score)
            weighted_score.append(avg_score)
            maximum.append(0.0)
            minimum.append(5.0)
    return (score, maximum, minimum, weighted_score, count)

# Process a line of sentence and return a field of score.
# In the return field, maximum is the maximum score of the word in the sentence.
# Similar to the minimum. The average is the mathematical average of all the score
# of word in the sentence. The weighted score is the weighted average of the score
# on the standard deviation of the word. 
def processline(cursor, content, st, avg_score, std_dev):
    content = content.lower()
    content = re.sub('n\'t', 'n', content)
    content = re.sub('[^0-9a-zA_Z]+',' ', content)
    split_content = nltk.word_tokenize(content)
    tagged_content = nltk.pos_tag(split_content)

    word_stat = []
    
    for content in tagged_content:
        word = content[0]
        tag = content[1]
        if tag in tags:
            cursor.execute("SELECT star_sum/number from word_star WHERE word like '" + str(word) + "';")
            records = cursor.fetchall()
            if (len(records) < 1):
                word_stat.append(avg_score)
            else:
                word_stat.append(float(str(records[0][0])))

    count = len(word_stat)
    dev_sum = 0;
    weighted_sum = 0;
    score_sum = 0.0
    score_max = 0.0
    score_min = 5.0
    if (count > 0):
        for x in range(0, count):
            data = word_stat[x]
            score_sum = score_sum + data;
            if (data > score_max):
                score_max = data
            if (data < score_min):
                score_min = data
            dev_sum = dev_sum + abs(data - avg_score) / std_dev
        for x in range(0, count):
            data = word_stat[x]
            weighted_sum = weighted_sum + data * abs(data - avg_score) / std_dev / dev_sum
        return (count, score_sum, score_max, score_min, weighted_sum)
    else:
        return (0, 0, 5.0, 0.0, 0)

# This function print the final result.  
def printResult(page, score, avg_score, std_dev):
    print "==================================================================\n"
    print str(avg_score) + " is the average score of all the words in database."
    print str(std_dev) + " is the standard deviation of all the data.\n"
    for x in range(0, len(page)):
        print "The sentence " + str(x + 1) + ": " + page[x] + "\tValuable Word Length: " + str(score[4][x]) + "\n\tMaximum Score: " + str(score[1][x]) + "\n\tAverage Score: " + str(score[0][x]) + "\n\tMinimum Score: " + str(score[2][x]) + "\n\tWeighted Score: " + str(score[3][x])       

if __name__ == "__main__":
    connect = dbConnect()
    conn = connect[1]
    cursor = connect[0]
    printUsage()
    page = sys.stdin.readlines()
    st = LancasterStemmer()
    cursor.execute("SELECT avg(star_sum/number), avg((star_sum / number) * (star_sum / number)) from word_star;")
    stat_data = cursor.fetchall()
    avg_score = stat_data[0][0]
    avg_score_square = stat_data[0][1]
    std_dev = (float(avg_score_square) - (float(avg_score) ** (2))) ** (1/2.0) 
    score = scoreSentence(cursor, page, st, avg_score, std_dev)
    printResult(page, score, avg_score, std_dev)
    cursor.close()
    conn.close()

# This file is used to produce the business vector used for later machine learning.
# Will directly insert the data into database.
# Business vector content:
#     From yelp_business table: business id, review count, followed by 
# average valuable word length;
#     From group result of the features(_test) table: weigthed score, 
# maximum score, minimum score and average from the user's review to th
# is business. 
#     From the yelp_chekin_flat table: the last one is the total checkin number.
#
# Notice: guest database account will not have right to modify the database.

import psycopg2
import sys
import re
import scoresentence

TEST = False # True if generating test data. False for training data.

if __name__ == '__main__':
    ret = scoresentence.dbConnect()
    conn = ret[1]
    cursor = ret[0]
    cursor.execute("SELECT * FROM categories;")
    cat = []
    for item1 in cursor.fetchall():
        cat.append(item1[1])
    if (TEST):
        end = "_test"
    else:
        end = ""

    cursor.execute("SELECT business_id, review_count from yelp_business;")
    store = cursor.fetchall()
    count = 0
    for item in store:
        count = count + 1
        print "Finish " + str(count) + " in " + str(len(store)) 
        bid = item[0]
        rc = item[1]
        cursor.execute("SELECT * from features" + end + " where bid = '" + str(bid) + "';")
        
        word_length = 0
        weighted_score = 0
        maximum = 0
        minimum = 5.0
        average = 0
        scores = cursor.fetchall()
        for score in scores:
            word_length = word_length + int(score[2])
            weighted_score = weighted_score + float(score[3])
            if (maximum < float(score[4])):
                maximum = float(score[4])
            if (minimum > float(score[5])):
                minimum = float(score[5])
            average = average + float(score[6])
        if (len(scores) != 0):
            word_length = word_length / len(scores)
            weighted_score = weighted_score / len(scores)
            average = average / len(scores)
        else:
            # The vector with no data will be assigned to a value which will not influence the ML
            word_length = -1
            weighted_score = -1
            average = -1
            maximum = -1
            minimum = -1
        
        if (word_length != -1):
            cursor.execute("SELECT * from yelp_checkin_flat where business_id = '" + str(bid) + "';")
            checkin = cursor.fetchone()
            checkin_sum = 0
            if (checkin != None):
                for x in range(1,169): # length of checkin
                    checkin_sum = checkin_sum + checkin[x]
                    
            query = "INSERT INTO business_features" + end + " values('" + str(bid) + "', " + str(rc) + ", " + str(word_length) + ", " + str(weighted_score) + ", " + str(maximum)  + ", " + str(minimum)  + ", " + str(average) + ", " + str(checkin_sum) + ");"
            cursor.execute(query)
            conn.commit()
            
    cursor.close()
    conn.close()

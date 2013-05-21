# This file is used to produce the user vector used for later machine learning.
# Will directly insert the data into database.
# User vector content:
#    From yelp_user_flat table: user id, average stars, review count, votes 
# funny, votes useful, votes cool;
#    From group result of the features(_test) table: average valuable
# review word length, average weighted score, maximum word score, minimum
# word score, and pure mathematical average.
#    From group result of the yelp_review_flat and selected category
# categories table: the categories user ever commented.
#
# Notice: guest database account will not have right to modify the database.

import psycopg2
import sys
import re
import scoresentence

TEST = False # True if generating test data. False for training data.

if __name__ == '__main__':
    connection = scoresentence.dbConnect()
    conn = connection[1]
    cursor = connection[0]
    cursor.execute("SELECT * FROM categories;")
    cat = []
    for item1 in cursor.fetchall():
        cat.append(item1[1])
    if (TEST):
        end = "_test"
    else:
        end = ""

    cursor.execute("SELECT distinct(uid) FROM features" + end + ";")
    uid_set = cursor.fetchall()
    count = 0
    for uid in uid_set:
        count = count + 1
        print "Running " + str(count) + " in " + str(len(uid_set))
        cursor.execute("SELECT b.categories FROM features" + end + " a, yelp_business b where a.bid = b.business_id and a.uid = '" + uid[0] + "';")
        store = cursor.fetchall()
        #print store
        cat_ret = []
        for item2 in store:
            raw_cat = item2[0]
            if 'Food' in raw_cat or 'Restaurants' in raw_cat:
                for tup in raw_cat:
                    if (tup != 'Food' and tup != 'Restaurants'):
                        if (tup in cat):
                            ret = cat.index(tup) + 1
                            if ret not in cat_ret:
                                cat_ret.append(ret)
                if len(cat_ret) == 0:
                    cat_ret.append(cat.index('Others'))
                
                cursor.execute("SELECT * from features" + end + " a where a.uid = '" + uid[0] + "';")
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
                word_length = word_length / len(scores)
                weighted_score = weighted_score / len(scores)
                average = average / len(scores)

        raw_cat = item2[0]
        if 'Food' in raw_cat or 'Restaurants' in raw_cat:       
            cursor.execute("SELECT b.user_id, b.average_stars, b.review_count, b.votes_funny, b.votes_useful, b.votes_cool from yelp_user_flat b where b.user_id = '" + uid[0] + "';");
            item3 = cursor.fetchone()
            if item3 != None:
                query = "INSERT INTO user_features" + end + " values('" + uid[0] + "', " + str(item3[1]) + ", " + str(item3[2]) + ", " + str(item3[3]) + ", " + str(item3[4]) + ", " + str(item3[5]) + ", " + str(word_length) + ", " + str(weighted_score) + ", " + str(maximum) + ", " + str(minimum) + ", " + str(average) + ", '" + str(cat_ret)[1:-1] + "');"
                cursor.execute(query)
                conn.commit()
    
    cursor.close()
    conn.close()

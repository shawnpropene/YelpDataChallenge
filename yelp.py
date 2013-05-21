#!/usr/bin/env python

from svm import *
from svmutil import *
from math import sqrt
import sys
import csv
import math
import psycopg2

TRAINING_QUERY = "select f.uid,f.bid,u.average_stars,u.review_count,u.votes_useful,u.votes_cool, u.votes_funny, u.word_length,u.maximum,u.minimum,u.average,u.category, b.review_count,b.word_length,b.weighted_score,b.maximum,b.minimum, b.average,b.checkin_sum from (select uid,bid from features group by uid,bid) f, user_features u, business_features b where f.uid = u.user_id and f.bid = b.bid;"
TESTING_QUERY = "select f.uid,f.bid,u.average_stars,u.review_count,u.votes_useful,u.votes_cool, u.votes_funny, u.word_length,u.maximum,u.minimum,u.average,u.category, b.review_count,b.word_length,b.weighted_score,b.maximum,b.minimum, b.average,b.checkin_sum from (select uid,bid from features_test group by uid,bid) f, user_features_test u, business_features_test b where f.uid = u.user_id and f.bid = b.bid limit 10;"


class Yelp:
    def __init__(self):
        self._features = []
        self._labels = []
        self._mean = []
        self._stddev = []
        self._model = None
        self._mode = "training"


    def dbConnect(self):
        conn_string = "host='abstract.cs.washington.edu' port='5252' dbname='yelp' user='yelp' password='yelp123'"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print "Succesfully Connected to database! \n"
        return (cursor, conn)


    def read_db(self, query):
        connect = self.dbConnect()
        cursor = connect[0]

        """ 
        The detailed explanation is as following:
        stars; user_id; business_id; user_average_stars; user_review_count; user_votes_useful; user_votes_cool; user_votes_funny; user_word_length; user_maximum; user_minimum; user_review_average; user_category; business_review_count; business_word_length; business_weighted_score; business_maximum; business_minimum; business_average; business_checkin_sum        
        """

        cursor.execute(query)   
        raw_features = cursor.fetchall() # Now, all the result (except the score) is in arrays in raw_features
        features = []
        labels = []
        count = 0
        maximum = len(raw_features)
        for feat in raw_features:
            count += 1
            print "\tprocessing %dth out of %d" % (count, maximum)
            # We will get the score in the following lines.
            uid = feat[0]
            bid = feat[1]
            query_get_score = "SELECT avg(stars) from yelp_review_flat where user_id = '" + str(uid) + "' and business_id = '" + str(bid) + "' group by user_id, business_id;"
            cursor.execute(query_get_score)
            score = cursor.fetchone()
            # Now we get the average stars (since there might be multiple reviews in database) of user giving to the business.
            
            # get all the features
            ftmp1 = [float(x) for x in feat[2:11]]
            ftmp2 = [float(x) for x in feat[12:]]
            categories = str(feat[11]).split(',')
            cate = 96 * [0]
            for x in categories:
                cate[int(x)] = 1
            ftmp= ftmp1 + cate + ftmp2

            features.append(ftmp)
            score = float(str(score)[1:-2])
            labels.append(math.ceil(score/0.5))

        print labels
        self._labels = labels
        self._features = features
        print "\tdone reading"
        return labels, features


    def standardize(self):
        features = self._features
        num_features = len(features[0]) 
        mean = self._mean
        stddev = self._stddev
        for i in range(0, num_features):
            for j in range(0, len(features)):
                if  stddev[i] != 0:
                    features[j][i] = (features[j][i] - mean[i]) / (2* stddev[i]) 
        
        return features


    def mean_stddev(self, features):
        num_features = len(features[0]) 
        print num_features
        
        tot = [0]*num_features
        mean = [0]*num_features
        stddev = [0]*num_features
        
        for i in range(0, num_features):
            for j in range(0, len(features)):
                tot[i] += features[j][i]
                
        for i in range(0, num_features):
            mean[i] = 1.0*tot[i]/len(features)
            
        print "mean computed"
        
        for i in range(0, num_features):
            err = 0
            for j in range(0, len(features)):
                err += pow((mean[i] - features[j][i]),2)
            err /= len(features)
            stddev[i] = sqrt(err)
            
        print "stddev computed"
        
        self._mean = mean
        self._stddev = stddev

        features = self.standardize()
        self._features = features
        
        return mean, stddev, features


    def train(self, kernel_type=RBF):
        labels = self._labels
        features = self._features
        [mean, stddev, features] = self.mean_stddev(features) 
        
        param = svm_parameter()
        param.kernel_type = kernel_type
        
        prob = svm_problem(labels, features)
        
        model = svm_train(prob, param)
        
        self._model = model

        print "done training"


    def save(self, model_path='model.t', mean_path='mean.txt', stddev_path='stddev.txt'):
        mean = self._mean
        stddev = self._stddev
        model = self._model
        svm_save_model(model_path, model)
        
        f = open(mean_path, 'w')
        for m in mean:
            f.write("%f\n" % m)
        f.close()
        
        f = open(stddev_path, 'w')
        for sd in stddev:
            f.write("%f\n" % sd)
        f.close()
        
        print "done saving"


    def load(self, model_path='model.t', mean_path='mean.txt', stddev_path='stddev.txt'):
        try:
            model = svm_load_model(model_path)
        except:
            raise "No model found"

        try:
            mean = []
            f = open(mean_path, 'r')
            for line in f:
                mean.append(float(line))
            f.close()
        except:
            raise "No mean file found"

        try:
            stddev = []
            f = open(stddev_path, 'r')
            for line in f:
                stddev.append(float(line))
            f.close()
        except:
            raise "No stddev file found "

        self._model = model
        self._mean = mean
        self._stddev = stddev

        return model, mean, stddev


    def test(self):
        labels = self._labels
        features = self._features
        self._features = self.standardize()
        a, b, c = svm_predict(labels, self._features, self._model)
        print a
        print labels


if __name__ == "__main__":
    yelp = Yelp()
    if (sys.argv[1] == "--train"):
        yelp.read_db(TRAINING_QUERY)
        yelp.train()
        yelp.save()
    elif (sys.argv[1] == "--test"):
        yelp.read_db(TESTING_QUERY)
        yelp.load()
        yelp.test()
    else:
        print "Unknown mode ", sys.argv[1] 

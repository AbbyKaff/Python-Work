# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 21:18:34 2020

@author: AK064060
"""

import pandas as pd
import numpy as np
from sklearn import datasets, linear_model
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import RepeatedKFold
from sklearn.metrics import f1_score
from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import make_classification
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB

# First attempt at a classification model on the data
#KNN
data = pd.read_csv('C:/Users/AK064060/Downloads/dataR2.csv')

x = data.drop(['Classification'], axis=1)
y = data.Classification

X_train, X_test, y_treain, y_test = train_test_split(x,y, test_size=0.3, random_state=42)
sc = StandardScaler()

sc.fit(X_train)

X_train_sd = sc.transform(X_train)
X_test_sd = sc.transform(X_test)

pd.DataFrame(X_train_sd, columns=data.columns).head()

knn = KNeighborsClassifier(n_neighbors=5, p=2, metric='minkowski')
knn.fit(X_train_sd, y_treain)

print('The Accuracy is at {:.2f}% on training data'.format(knn.score(X_train_sd, y_treain)*100))
print('The Accuracy is at {:.2f}% on test data'.format(knn.score(X_test_sd, y_test)*100))
#The Accuracy is at 80.25% on training data
#The Accuracy is at 80.00% on test data

cv = np.mean(cross_val_score(knn, X_train, y_treain, cv=10))
print ("Accuracy using KNN with 10 cross validation : {}%".format(round(cv*100,2)))
#Accuracy using KNN with 10 cross validation : 53.33%
y_predict_test = knn.predict(X_test)

score_test = f1_score(y_test, y_predict_test, pos_label=list(set(y_test)), average = None)
print(score_test)
#0.67924528
# with an F1 score of 0.68 we can see that this model does not do bad, however
# with its low accuracy in the cv I'd lik to attempt a different method


#Random Forest
data = pd.read_csv('C:/Users/AK064060/Downloads/dataR2.csv')

x = data.drop(['Classification'], axis=1)
y = data.Classification

X_train, X_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=42)

rfc = RandomForestClassifier(n_estimators=100, n_jobs=1, criterion='gini')
rfc.fit(X_train, y_train)

cv = np.mean(cross_val_score(rfc, X_train, y_train, cv=10))
print ("Accuracy using RF with 10 cross validation : {}%".format(round(cv*100,2)))
#Accuracy using RF with 10 cross validation : 68.06% 
y_predict_test = rfc.predict(X_test)

score_test = f1_score(y_test, y_predict_test, pos_label=list(set(y_test)), average = None)
print(score_test)
# F1 Score = 0.68571429
# A little bit better of an F1 score and Accuracy using cross validation, 
# but lets see if another method may get us closer


# Logistic Regression
data = pd.read_csv('C:/Users/AK064060/Downloads/dataR2.csv')

x = data.drop(['Classification'], axis=1)
y = data.Classification

X_train, X_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=42)

clf = LogisticRegression(random_state=0).fit(X_train, y_train)
clf.fit(X_train, y_train)
cv = np.mean(cross_val_score(clf, X_train, y_train, cv=10))
print ("Accuracy using Logistic Regression with 10 cross validation : {}%".format(round(cv*100,2)))
#Accuracy using Logistic Regression with 10 cross validation : 70.42%
y_predict_test = clf.predict(X_test)

score_test = f1_score(y_test, y_predict_test, pos_label=list(set(y_test)), average = None)
print(score_test)
# 0.78947368
# As we can see with the accuracy and the F1 score we are becoming better as we go

# Gradient boosting
data = pd.read_csv('C:/Users/AK064060/Downloads/dataR2.csv')

x = data.drop(['Classification'], axis=1)
y = data.Classification

X_train, X_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=42)

gbc = GradientBoostingClassifier(random_state=0)
gbc.fit(X_train, y_train)
gbc.score(X_test,y_test)

cv = np.mean(cross_val_score(gbc, X_train, y_train, cv=10))
print ("Accuracy using Gradient Boosting with 10 cross validation : {}%".format(round(cv*100,2)))
#Accuracy using Gradient Boosting with 10 cross validation : 73.06%
y_predict_test = gbc.predict(X_test)

score_test = f1_score(y_test, y_predict_test, pos_label=list(set(y_test)), average = None)
print(score_test)
#0.68421053
# This looks like the accuracy may be a bit better than the others but the F1 score seemed to drop

# Naive Bayes
data = pd.read_csv('C:/Users/AK064060/Downloads/dataR2.csv')

x = data.drop(['Classification'], axis=1)
y = data.Classification

X_train, X_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=42)

gnb = GaussianNB()
y_pred = gnb.fit(X_train, y_train).predict(X_test)

print("Number of mislabeled points out of a total %d points : %d"% (X_test.shape[0], (y_test != y_pred).sum()))
#Number of mislabeled points out of a total 35 points : 12
cv = np.mean(cross_val_score(gnb, X_train, y_train, cv=10))
print ("Accuracy using Gradient Boosting with 10 cross validation : {}%".format(round(cv*100,2)))
#Accuracy using Gradient Boosting with 10 cross validation : 60.69%
y_predict_test = gnb.predict(X_test)

score_test = f1_score(y_test, y_predict_test, pos_label=list(set(y_test)), average = None)
print(score_test)
#  0.68421053
# This method does not seem to be working as well as some of the others
# After running multiple different models to see which performed best with cross-validation
# 10 fold, I'd say that Logistic Regression would be the way to go, based on how it performed
# compared to some of the other methods with both the Accuracy and the F1 score. 

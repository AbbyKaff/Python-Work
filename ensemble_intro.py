#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Jeffrey Thompson
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
import statsmodels.formula.api as smf
from sklearn.neighbors import KNeighborsRegressor
from sklearn.model_selection import KFold

def MSE(preds, true):
    mse = sum([(true[i] - preds[i])**2 for i in range(len(true))]) / len(true)
    return(mse)

#dat = pd.read_excel('../Data/Concrete_Data.xls')
#dat.columns = ['Var' + str(i) for i in range(1,10,1)]

#np.random.seed(1)
#train_dat, test_dat = train_test_split(dat, test_size=0.3333)
train_dat = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_train.tsv", sep='\t')
test_dat = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_test.tsv", sep='\t')
y_test = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_test.tsv", sep='\t')
y_test = y_test.O3
X_test = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_test.tsv", sep='\t').drop(['O3'], axis=1)


y_train = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_train.tsv", sep='\t')
y_train = y_train.O3
X_train = pd.read_csv(r"C:\Users\AK064060\Downloads\summer_train.tsv", sep='\t').drop(['O3'], axis=1)

mod1 = DecisionTreeRegressor(random_state=1,
                            max_depth=5,
                            min_samples_leaf=5,
                            criterion='mse').fit(X_train, y_train)
mod2 = smf.ols(formula='O3 ~ NO2 + NO + SO2 + PM10', data=train_dat).fit()
mod3 = KNeighborsRegressor(n_neighbors=1).fit(X_train, y_train)

preds1 = mod1.predict(X_test)
preds2 = mod2.predict(test_dat)
preds3 = mod3.predict(X_test)

MSE(preds1, y_test) #65.12212360850133
MSE(preds2.values, y_test) #63.51349551021724
MSE(preds3, y_test) #130.9378238341969

preds = np.hstack((preds1.reshape(-1,1), preds2.values.reshape(-1,1), preds3.reshape(-1,1)))
preds = np.mean(preds, axis=1)
MSE(preds, y_test) #65.76274464931818


# Cross-validation comparison for emsemble methods.
cv_obj = KFold(n_splits=10, shuffle=True, random_state=1)
dat = pd.concat([train_dat,test_dat])
folds = cv_obj.split(dat)

res_three = []
res_bagging = []

for fold in folds:
    X1 = dat.iloc[fold[0],:]
    y1 = X1['O3']
    X1 = X1.drop('O3', axis=1)
    X2 = dat.iloc[fold[1],:]
    y2 = X2['O3']
    X2 = X2.drop('O3', axis=1)
    dat1 = dat.iloc[fold[0],:]
    dat2 = dat.iloc[fold[1],:]
    
    mod1 = DecisionTreeRegressor(random_state=1,
                            max_depth=5,
                            min_samples_leaf=5,
                            criterion='mse').fit(X1, y1)
    mod2 = smf.ols(formula='O3 ~ NO2 + NO + SO2 + PM10', data=dat1).fit()
    mod3 = KNeighborsRegressor(n_neighbors=1).fit(X1, y1)
    
    preds1 = mod1.predict(X2)
    preds2 = mod2.predict(dat2)
    preds3 = mod3.predict(X2)
    
    preds = np.hstack((preds1.reshape(-1,1), preds2.values.reshape(-1,1), preds3.reshape(-1,1)))
    preds = np.mean(preds, axis=1)
    
    y2 = y2.values
    
    res_three.append(MSE(preds, y2))
    
    
    preds = np.ones((len(y2), 1))

    for i in range(1000):
        np.random.seed(i)
        bag_dat = dat1.sample(n = int(0.5 *len(dat1)), replace=True)
        y1 = bag_dat['O3']
        X1 = bag_dat.drop('O3', axis=1)
        mod = DecisionTreeRegressor(random_state=1,
                                    max_depth=5,
                                    min_samples_leaf=5,
                                    criterion='mse').fit(X1, y1)
        preds = np.hstack((preds, mod.predict(X2).reshape(-1,1)))
    
    preds_bag = preds[:,1:1001]
    preds_bag = np.mean(preds_bag, axis=1)

    res_bagging.append(MSE(preds_bag, y2))

np.mean(res_three)    
np.mean(res_bagging)

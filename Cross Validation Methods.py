# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 19:09:41 2020

@author: abiga
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
import statsmodels.stats.anova as anova
from sklearn.model_selection import train_test_split
import random
import seaborn as sns
from sklearn import neighbors
import os

os.chdir(r'\Users\abiga\Documents\DATA 881\Week 5')

temp_dat = pd.read_table('./Auto(2)(1).tsv', delim_whitespace=True)
temp_dat = temp_dat.groupby('mpg').mean()
temp_dat['mpg2'] = [int(x) for x in temp_dat.index]
#temp_dat.columns = ['mpg2', 'cylinders', 'horsepower', 'weight']
#temp_poly['MeanTemp'] = temp_dat['MeanTemp'].values

# Build different models, for different polynomial degrees.
mod1 = smf.ols(formula = 'mpg2 ~ cylinders', data=temp_dat).fit()
mod2 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower * weight', data=temp_dat).fit()
mod3 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower + weight', data=temp_dat).fit()
mod4 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower * weight', data=temp_dat).fit()
mod5 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower + weight', data=temp_dat).fit()

# Test for differences between models.
anova.anova_lm(mod1, mod2)
anova.anova_lm(mod2, mod3)
anova.anova_lm(mod3, mod4)
anova.anova_lm(mod4, mod5)


temp_dat_train, temp_dat_test = train_test_split(temp_dat, test_size=0.333)

mod1 = smf.ols(formula = 'mpg2 ~ cylinders', data=temp_dat_train).fit()
mod2 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower + weight', data=temp_dat_train).fit()
mod3 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower * weight', data=temp_dat_train).fit()
mod4 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower + weight', data=temp_dat_train).fit()
mod5 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower * weight', data=temp_dat_train).fit()

# Get the training MSE.
mod1.mse_resid
mod2.mse_resid
mod3.mse_resid
mod4.mse_resid
mod5.mse_resid

# step 2. build a function (e.g. lm_mod) to call different linear model with arguments like:  data to use, lm formula 

# lm_mod = function( )                       
 
# step 3. build a cv function with arguments like: data to use, response variable, lm formula, fold, seed

# cv = function( ) 

# step 4. call cv function, passing arguments of data to use, response variable, lm formula using as.formula, fold, seed 
# temp_dat = pd.read_table('./Auto(2)(1).tsv', delim_whitespace=True)
# temp_dat = temp_dat.groupby('mpg').mean()
# temp_dat['mpg2'] = [int(x) for x in temp_dat.index]
#temp_dat.columns = ['mpg2', 'cylinders', 'horsepower', 'weight']
#temp_poly['MeanTemp'] = temp_dat['MeanTemp'].values

# Build different models, for different polynomial degrees.
mod1 = smf.ols(formula = 'mpg2 ~ cylinders', data=temp_dat).fit()
mod2 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower * weight', data=temp_dat).fit()
mod3 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower + weight', data=temp_dat).fit()
mod4 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower * weight', data=temp_dat).fit()
mod5 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower + weight', data=temp_dat).fit()

# Test for differences between models.
anova.anova_lm(mod1, mod2)
anova.anova_lm(mod2, mod3)
anova.anova_lm(mod3, mod4)
anova.anova_lm(mod4, mod5)


temp_dat_train, temp_dat_test = train_test_split(temp_dat, test_size=0.333)

mod1 = smf.ols(formula = 'mpg2 ~ cylinders', data=temp_dat_train).fit()
mod2 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower + weight', data=temp_dat_train).fit()
mod3 = smf.ols(formula = 'mpg2 ~ cylinders + horsepower * weight', data=temp_dat_train).fit()
mod4 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower + weight', data=temp_dat_train).fit()
mod5 = smf.ols(formula = 'mpg2 ~ cylinders * horsepower * weight', data=temp_dat_train).fit()

# Get the training MSE.
mod1.mse_resid
mod2.mse_resid
mod3.mse_resid
mod4.mse_resid
mod5.mse_resid


# step 5. calculate mean test_mse

def ortho_poly_fit(x, degree = 1):
    n = degree + 1
    x = np.asarray(x).flatten()

    xbar = np.mean(x)
    x = x - xbar
    X = np.fliplr(np.vander(x, n))
    q,r = np.linalg.qr(X)

    z = np.diag(np.diag(r))
    raw = np.dot(q, z)

    norm2 = np.sum(raw**2, axis=0)
    alpha = (np.sum((raw**2)*np.reshape(x,(-1,1)), axis=0)/norm2 + xbar)[:degree]
    Z = raw / np.sqrt(norm2)
    return Z, norm2, alpha

temp_dat = pd.read_table('./Auto(2)(1).tsv', delim_whitespace=True)

#temp_dat['Year'] = temp_dat['dt'].str.slice(start=0, stop=4)
temp_dat = temp_dat.groupby('mpg').mean()
#temp_dat = temp_dat.rename(columns={'LandAverageTemperature':'MeanTemp'})
temp_dat['mpg2'] = [int(x) for x in temp_dat.index]

#temp_dat = temp_dat[temp_dat['Year'] > 1849]

plt.scatter(temp_dat['mpg2'], temp_dat['cylinders'])

mod1 = smf.ols(formula = 'mpg2 ~ cylinders', data=temp_dat).fit()
mod1.summary()

temp_poly = ortho_poly_fit(temp_dat['mpg2'], temp_dat['cylinders'], 
                           temp_dat['horsepower'], temp_dat['weight'])
temp_poly = pd.DataFrame(temp_poly[0])
temp_poly.columns = ['mpg', 'mpg_1', 'mpg_2', 'mpg_3', 'mpg_4', 'mpg_5']
temp_poly['Meanmpg'] = temp_dat['mpg2'].values

def test_mse(mod, dat, response, pred):
    preds = mod.predict(dat)
    test_mse = sum([x**2 for x in (dat[response] - preds)])/len(preds)
    return(test_mse)


def poly_mod(dat, response, pred, deg=1):
    f = response + ' ~ '
    for i in range(1, deg+1):
        f = f + pred + '_' + str(i)
        if i < deg:
            f = f + ' + '
    mod = smf.ols(formula = f, data = dat).fit()
    return(mod)
    
def knn_mod(dat, response, pred, k=1):
    pred = pred + '_1'
    mod = neighbors.KNeighborsRegressor(n_neighbors = k)
    mod.fit(dat[pred].values.reshape(-1,1), dat[response])
    preds = mod.predict(dat[pred].values.reshape(-1,1))
    mod.mse_resid = sum([x**2 for x in (dat[response] - preds)])/len(preds)
    return(mod)

def knn_test(mod, dat, response, pred):
    pred = pred + '_1'
    preds = mod.predict(dat[pred].values.reshape(-1,1))
    test_mse = sum([x**2 for x in (dat[response] - preds)])/len(preds)
    return(test_mse)

def which(cond):
    return([i for i in range(len(cond)) if cond[i]])

def cv(dat, response, pred, deg=1, k=5, seed=1, TRAINFUN=poly_mod, TESTFUN=test_mse):
    random.seed(seed)
    
    folds = pd.cut([x for x in range(len(dat))], bins=k)
    random.shuffle(folds)
    folds = folds.codes
    
    folds_df = pd.DataFrame({'TrainMSE': [],
                             'TestMSE': [],
                             'Fold': [],
                             'Degree': []})
    
    for i in range(k):
        test = which([x == i for x in folds])
        train = set([x for x in range(len(dat))]).difference(test)
        train = [x for x in train]
        
        train_mse_list = []
        test_mse_list = []
        
        for j in range(1, deg+1):
            mod = TRAINFUN(dat.iloc[train], response, pred, j)
            train_mse_list.append(mod.mse_resid)
            test_mse_list.append(TESTFUN(mod, dat.iloc[test], response, pred))
        
        fold = pd.DataFrame({'TrainMSE': train_mse_list,
                             'TestMSE': test_mse_list,
                             'Fold': i,
                             'Degree': [x for x in range(1, deg+1)]})
        folds_df = folds_df.append(fold)
    
    return(folds_df)




res = cv(temp_dat, 'mpg2', 'cylinders', deg=5, seed=1)

res_m = pd.melt(res, id_vars = ['Fold', 'Degree'])

res2 = cv(temp_poly, 'MeanTemp', 'Year', deg=5, seed=1, TRAINFUN=knn_mod, TESTFUN = knn_test)

res2_m = pd.melt(res2, id_vars = ['Fold', 'Degree'])
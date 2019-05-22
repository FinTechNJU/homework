#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 15:37:25 2019

@author: lilangyi
"""

import numpy as np 
import pandas as pd
from pandas import DataFrame

from sklearn import tree
from sklearn import preprocessing
from sklearn.model_selection import KFold
#import math
from sklearn.metrics import roc_auc_score



def loadData(path):
    
    fr=open("/Users/lilangyi/Downloads/"+path)
    line= fr.readline()
    if line[0]=="|":
        pass
    else:
        fr=open("/Users/lilangyi/Downloads/"+path)
        
    rd=[]
    #fr=open("/Users/lilangyi/Downloads/"+path)
    for line in fr.readlines():
        #print(line)
        arr=line.strip().split(", ")
        #print(arr)
        rd.append(arr)
    #yl,xl=len(rd),len(rd[0])
    xMat=DataFrame(rd)  
    #xMat=xMat.applymap(str) 
    lst=[0,2,4,10,11,12]
    for i in lst:
        xMat[i]=xMat[i].astype(np.int64)    
    for i in range(14):
        if i not in lst:
            #xMat[i]=xMat[i].apply(str)
            #xMat[i]=xMat[i].astype(str)
            xMat[i]=xMat[i].apply(hash)
    #print(xMat)  
    xMat[14]=xMat[14].replace("<=50K",-1)
    xMat[14]=xMat[14].replace(">50K",1)
    xMat[14]=xMat[14].replace("<=50K.",-1)
    xMat[14]=xMat[14].replace(">50K.",1)
    #print(xMat)    
    return xMat.iloc[:,:-1],xMat.iloc[:,-1]
    
dataMat,labelMat=loadData("test.data")
testMat,testlabel=loadData("test.test") 
print("Data Loaded!")


#
def randomforest(dataMat,labelMat,testdata,forecast_data,num_trainer):
    trainer_result=[]
    forecast_result=[]
    for i in range(num_trainer):
        clf = tree.DecisionTreeClassifier(criterion="entropy",max_depth=1,max_features="auto")
        clf.fit(dataMat,labelMat)
        
        trainer_pro=clf.predict_proba(testdata)
        trainer_pro_z=[]
        for item in trainer_pro:
            trainer_pro_z.append(item[1])
        trainer_result.append(trainer_pro_z)
        
        forcast_pro=clf.predict_proba(forecast_data)
        forcast_pro_z=[]
        for item in forcast_pro:
            forcast_pro_z.append(item[1])
        forecast_result.append(forcast_pro_z)
        
    return trainer_result,forecast_result

#5折交叉
auc_z=[]
for trainer_num in range(1,10):
    auc=0
    kfold= KFold(n_splits=5,random_state =None)
    for train,test in kfold.split(dataMat):
        
        train_result,forecast_result=\
        randomforest(dataMat[train],\
        labelMat[train],\
        dataMat[test],\
        testMat,\
        trainer_num)
        
        for i in range(trainer_num):
            if(i==0):
                trainer_result_z=np.array(trainer_result[i])/trainer_num
            else:
                trainer_result_z=trainer_result_z+np.array(trainer_result[i])/trainer_num
           
        auc+=roc_auc_score(np.array(labelMat)[test],trainer_result_z)
    auc_z.append(auc/5)

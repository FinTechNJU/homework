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

def ada_train(dataMat,labelMat,testdata,forecast_data,num_trainer):
    forecast_result=[]
    trainer_result=[]
    D=[]#初始化样本权重
    alpha_list=[]
    count=np.shape(dataMat)[0]
    for i in range(count):
        D.append(1/count)
        
    clf = tree.DecisionTreeClassifier(criterion="gini",max_depth=1)    
    for i in range(num_trainer):
        clf.fit(dataMat,labelMat,sample_weight=np.array(D))
        mytrain=clf.predict(dataMat)
        mysum=0
        for k in range(len(mytrain)):
            if(mytrain[k]!=labelMat[k]):
                mysum+=D[k]
        e=mysum
        if(e>0.5):
            break
        trainer_pro=clf.predict_proba(testdata)
        trainer_pro_z=[]
        for item in trainer_pro:
            trainer_pro_z.append(item[1])
        trainer_result.append(trainer_pro_z)
        

        alpha=0.5*math.log((1-e)/(e))
        alpha_list.append(alpha)
        
        forcast_pro=clf.predict_proba(forecast_data)
        forcast_pro_z=[]
        for item in forcast_pro:
            forcast_pro_z.append(item[1])
        forecast_result.append(forcast_pro_z)
        
 #更新样本权重
        Z=0
        for j in range(len(D)):
            Z+=float(D[j]*math.exp(-alpha*mytrain[j]*labelMat[j]))
        for j in range(len(D)):
            D[j]=D[j]*math.exp(-alpha*mytrain[j]*labelMat[j])
    return trainer_result,alpha_list,forecast_result,i+1

#5折交叉
trainer_num=1
auc_z=[]

for trainer_num in range(1,20):
    auc=0
    kfold= KFold(n_splits=5,random_state =None)
    for train,test in kfold.split(dataMat):
        trainer_result,alpha_list,forecast_result,count=ada_train(list(np.array(dataMat)[train]),
                  list(np.array(labelMat)[train]),list(np.array(dataMat)[test]),testMat,trainer_num)
        for i in range(count):
            if(i==0):
                trainer_result_z=np.array(trainer_result[i])*alpha_list[i]/sum(alpha_list)
            else:
                trainer_result_z=trainer_result_z+np.array(trainer_result[i])*alpha_list[i]/sum(alpha_list)
           
        auc+=roc_auc_score(np.array(labelMat)[test],trainer_result_z)
    auc_z.append(auc/5)
    

#测试集分类结果

for i in range(count):
    if(i==0):
            result=np.array(forecast_result[i])*alpha_list[i]/sum(alpha_list)
    else:
            result=result+np.array(forecast_result[i])*alpha_list[i]/sum(alpha_list)
forecast_auc=roc_auc_score(np.array(testlabel),result)
print("result:%f"%forecast_auc)
    

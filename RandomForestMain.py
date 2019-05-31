from __future__ import division
from pandas import DataFrame
from sklearn import tree
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import KFold

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random

def loadData(path):  
    fr=open("/Users/lilangyi/Downloads/"+path)
    line= fr.readline()
    if line[0]=="|":
        pass
    else:
        fr=open("/Users/lilangyi/Downloads/"+path)     
    rd=[]
    for line in fr.readlines():
        arr=line.strip().split(", ")
        rd.append(arr)
    xMat=DataFrame(rd)  
    for i in range(14):
        xMat[i]=xMat[i].apply(hash)
        xMatIMax=max(xMat[i])
        xMat[i]=xMat[i].apply(lambda x:x/xMatIMax)
        xMat[i]=xMat[i].apply(lambda x:float("%.2f"%x))
    xMat[14]=xMat[14].replace("<=50K",-1)
    xMat[14]=xMat[14].replace(">50K",1)
    xMat[14]=xMat[14].replace("<=50K.",-1)
    xMat[14]=xMat[14].replace(">50K.",1)  
    return np.mat(xMat.iloc[:-1,:-1]),np.array(xMat.iloc[:-1,-1])

class RandomForest:

    def __init__(self,trCounts=10,trDimension=0.5):
        self.treeCounts=trCounts
        self.treeDimension=trDimension
        self.treeList=[]
        
    # 随机抽取样本，样本数量与原训练样本集一样，维度为 int(pow(m - 1,self.treeDimension))
    def baggingDataSet(self,dataSet):
        #样本自助采样，随机选择特征
        n, m = dataSet.shape
        features = random.sample(dataSet.columns.values[:-1].tolist(), int(pow(m - 1,self.treeDimension)))
        features.append(dataSet.columns.values[-1])
        rows = [random.randint(0, n-1) for _ in range(n)]
        trainData = dataSet.iloc[rows][features[:-1]]
        trainLabel = dataSet.iloc[rows][features[-1]]
        return trainData, trainLabel
        
    def rfTrain(self,dataMat,dataLabel): 
        for i in range(self.treeCounts):      
            decisionTree = tree.DecisionTreeClassifier(max_depth=5) 
            decisionTree.fit(np.mat(dataMat), np.array(dataLabel))
            self.treeList.append([decisionTree,\
                             dataMat.columns.values,
                             dataLabel.index.values])
        return
    
    def rfPredict(self,predMat):   
        # 对测试样本分类
        labelPred = []
        for tr in self.treeList:
            testLabel = tr[0].predict(predMat.T[tr[1]].T)
            labelPred.append(testLabel.astype(int))         
        predLabel=[]
        for i in range(len(labelPred[0])):
            tmpList=[]
            for item in labelPred:
                tmpList.append(item[i])
            # 投票选择最终类别
            labelDict = {}
            for label in tmpList:
                labelDict[label] = labelDict.get(label, 0) + 1            
            sortClass = sorted(labelDict.items(), key=lambda item: item[1])
            predLabel.append(sortClass[-1][0])     
        return np.array(predLabel)
    
    def rfEvaluation(self,testMat,testLabel):
        rfClassEst,expon = 0, 0.4
        predLabel = self.rfPredict(testMat)
        rfClassEst = roc_auc_score(testLabel,predLabel)
        return rfClassEst**expon
    

if __name__=="__main__":
    print(">>>Process 1:   The processing of data\n>>>Description: To load the given data")
#    fakeData,realData=True,False
    realData,fakeData=True,False
    
    if(fakeData):
        dataMat,labelMat=loadData("test.data")
        testMat,testlabel=loadData("test.test") 
    elif(realData):
        dataMat,labelMat=loadData("adult.data")
        testMat,testlabel=loadData("adult.test")  
    df = pd.concat([pd.DataFrame(dataMat),pd.DataFrame(labelMat)],axis=1)
    df=np.mat(df)
    df=pd.DataFrame(df)
    print(">>>Result:      Data loaded!\n")
    
    print(">>>Process 2:   The KFold of data\n>>>Description: To process the loaded data")
    numTrees, stepGap = 50, 1
    treeDimension, i= 0.5, 0
    forestList=[]
    kfold= KFold(n_splits=5,random_state =None)    
    numTrees+=i*stepGap
    treeDimension+=0.05*stepGap
    rf = RandomForest(numTrees,treeDimension) #生成对象
    baggingData, bagginglabels = rf.baggingDataSet(df)
    for train,test in kfold.split(dataMat):
        numTrees+=i*stepGap
        rf.treeDimension+=0.05*stepGap
        rf.rfTrain(baggingData, bagginglabels)
        aucEst = rf.rfEvaluation(testMat,testlabel)
        forestList.append({"rf":rf,"numTrees":numTrees,\
                           "aucEst":aucEst})
        i+=1
    aucList = []
    for item in forestList:
        aucList.append(item["aucEst"])
    aucIndex=aucList.index(max(aucList))    
    print(">>>Detail:      The AUC of the test set is : %f "%aucList[aucIndex])

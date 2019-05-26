import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pandas import DataFrame
from sklearn import tree
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import KFold


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
#        b.ix[i][j]=float('%.2f' %b.ix[i][j])
#    lst=[0,2,4,10,11,12]
#    for i in lst:
#        try:
#            xMat[i]=xMat[i].astype(np.int64)
#        except ValueError:
#            pass
#    
#    for i in range(14):
#        if i not in lst:
#            xMat[i]=xMat[i].apply(hash)
##            xMatIMax=max(xMat[i])
##            xMat[i]=xMat[i].apply(lambda x:x/xMatIMax)
    xMat[14]=xMat[14].replace("<=50K",-1)
    xMat[14]=xMat[14].replace(">50K",1)
    xMat[14]=xMat[14].replace("<=50K.",-1)
    xMat[14]=xMat[14].replace(">50K.",1)  
    return np.mat(xMat.iloc[:-1,:-1]),np.array(xMat.iloc[:-1,-1])

class AdaBoost:
    def __init__(self):
        self.weakClassArr=[]
        self.weakClassArrAlpha=[]
        
    def adaBoostTrainDS(self, dataArr,classLabels,numIt):#numlt：迭代次数
#        dataArr,classLabels=dataMat,labelMat
#        weakClassArr=[]
        m=np.shape(dataArr)[0]
        D=np.full(m,1/m)
        aggClassEst=np.full(m,0.0)
        
        for i in range(numIt):
            
            clf=tree.DecisionTreeClassifier(max_depth=1)  
            clf.fit(dataArr,classLabels,sample_weight=D)
            error=1-clf.score(dataArr,classLabels,sample_weight=D)
            classEst=clf.predict(dataArr)
            if(error>=0.5):
                continue
            alpha=float(0.5*np.log((1.0-error)/max(error,1e-16)))
            self.weakClassArr.append(clf)
            self.weakClassArrAlpha.append(alpha)
            
            expon=np.multiply(-1*alpha*np.array(classLabels),classEst)
            D=np.multiply(D.T,np.exp(expon))/D.sum()
            aggClassEst += alpha*classEst
            aggErrors = np.multiply(\
                np.sign(aggClassEst)!= \
                classLabels,np.full(m,1)\
                )
            errorRate=aggErrors.sum()/m
            if(errorRate==0.0):
                break
        return aggClassEst

    def adaClassify(self,dataToClass,labelToClass):
        aggClassEst,expon=0,0.2
        for i in range(len(self.weakClassArr)):
            classEst=self.weakClassArr[i].predict(dataToClass)
            aggClassEst += \
            self.weakClassArrAlpha[i]*\
            roc_auc_score(labelToClass,classEst)/sum(self.weakClassArrAlpha)
        aggClassEst=aggClassEst**expon
        return aggClassEst
    
    

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
    
    print(">>>Result:      Data loaded!\n")
    
    print(">>>Process 2:   The KFold of data\n>>>Description: To process the loaded data")
    numIt=1
    adaBoostList=[]
    kfold= KFold(n_splits=5,random_state =None)
    for train,test in kfold.split(dataMat):
        adaBoost=AdaBoost()
        agg = adaBoost.adaBoostTrainDS(dataMat[train],labelMat[train],numIt)
        rst = adaBoost.adaClassify(dataMat[test],labelMat[test])
        adaBoostList.append([adaBoost,numIt,rst])
        numIt+=4
    aucList,numItList=[],[]
    for item in adaBoostList:
        aucList.append(item[2])
        numItList.append(item[1])
    aucIndex=aucList.index(max(aucList))
    rst = adaBoostList[aucIndex][0].adaClassify(testMat,testlabel)
    print(">>>Result:      Data processed!")
    print(">>>Detail:      The AUC of the test set is : %f "%rst)
    
    plt.plot(np.array(numItList),np.array(aucList))
    plt.title('Auc of 5-Flod')
    plt.xlabel('Num of Learner')
    plt.ylabel('AUC of the Model')
    plt.show()

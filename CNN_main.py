# -*- coding: utf-8 -*-
"""

参考网页：
1. https://blog.csdn.net/qq_34714751/article/details/85610966
2. https://github.com/liamlycoder/PyTorch_Primer/blob/master/PyTorch_Primer/05CNNonMNIST/work.py

"""

import torch
import torch.nn as nn
import torchvision
import torch.utils.data as Data

    DOWNLOAD_MNIST = True  
    # Mnist 手写数字
    train_data = torchvision.datasets.MNIST(
        root='./mnist/',  
        train=True,  
        transform=torchvision.transforms.ToTensor(),  
        download=DOWNLOAD_MNIST,  
    )#60000*28*28
    test_data = torchvision.datasets.MNIST(
        root='./mnist/',
        train=False,
    )#10000*28*28

        
    EPOCH = 1  # 训练次数
    BATCH_SIZE = 50#每批数量
    LR = 0.001   # 学习率
    train_loader = Data.DataLoader(
        dataset=train_data,
        batch_size=BATCH_SIZE,
        shuffle=True  # 是否打乱顺序
    )
    test_loader = Data.DataLoader(
            dataset=train_data,
            batch_size=BATCH_SIZE,
        )


#CNN创建
class CNN(nn.Module):
    def __init__(self):
        super(CNN,self).__init__()
        self.conv1 = torch.nn.Sequential(
            torch.nn.Conv2d(in_channels=1,
                            out_channels=16,
                            kernel_size=3,
                            stride=2,
                            padding=1),
            torch.nn.BatchNorm2d(16),
            torch.nn.ReLU()
        )
        self.conv2 = torch.nn.Sequential(
            torch.nn.Conv2d(16,32,3,2,1),
            torch.nn.BatchNorm2d(32),
            torch.nn.ReLU()
        )
        self.conv3 = torch.nn.Sequential(
            torch.nn.Conv2d(32,64,3,2,1),
            torch.nn.BatchNorm2d(64),
            torch.nn.ReLU()
        )
        self.conv4 = torch.nn.Sequential(
            torch.nn.Conv2d(64,64,2,2,0),
            torch.nn.BatchNorm2d(64),
            torch.nn.ReLU()
        )
        self.mlp1 = torch.nn.Linear(2*2*64,100)
        self.mlp2 = torch.nn.Linear(100,10)
    def forward(self, x):
        x = self.conv1(x)
        x = self.conv2(x)
        x = self.conv3(x)
        x = self.conv4(x)
        x = self.mlp1(x.view(x.size(0),-1))
        x = self.mlp2(x)
        return x
    
    
cnn = CNN()
train_loss = 0
eval_loss = 0

optimizer = torch.optim.SGD(cnn.parameters(), lr=LR)  #随机梯度下降
loss_function = nn.CrossEntropyLoss()

losses = []#训练损失
eval_losses = []#测试损失
eval_acces = []#测试准确率

for i, (b_x, b_y) in enumerate(train_loader,0):
    output = cnn(b_x)
    loss = loss_function(output, b_y)
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
    train_loss += loss.item()
    if i % 300 == 299:
        losses.append(train_loss / 300)
        train_loss=0
        for (test_x,test_y) in test_loader:
            test_out = cnn(test_x)
            eval_loss += loss_function(test_out,test_y).item()
        eval_losses.append(eval_loss/len(test_data))
            
test_a = torch.unsqueeze(test_data.data, dim=1).type(torch.FloatTensor)
test_b = test_data.targets

test_out = cnn(test_a)
predict_y = torch.argmax(test_out, 1).data.numpy()
print('Accuracy in Test : %.4f%%' % (sum(predict_y == test_b.data.numpy()) * 100/ len(predict_y)))

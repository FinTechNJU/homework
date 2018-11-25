<h1>实验三实验报告</h1>
实验报告内容要求：
实验设计说明，包括主要设计思路、算法设计、程序和各个类的设计说明
程序运行和实验结果说明和分析
性能、扩展性等方面存在的不足和可能的改进之处
<h3>需求一</h3>
<h4>实验目的：</h4>
针对股票新闻数据集中的新闻标题，编写WordCount程序，统计所有除Stop-word（如“的”，“得”，“在”等）出现次数k次以上的单词计数，最后的结果按照词频从高到低排序输出。
<h4>实验的设计思路</h4>
包括两个mapreduce
此实验基于wordcount程序。首先进行分词，然后得到词频。然后，对这些键值对进行排序。
其中，调用了word.jar来实现了分词的功能。排序的功能采用了InverseMapper.class（书上P118页）
<h4>算法设计以及各类分析</h4>

``` java 
public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>
```

此处实现了读取新闻的标题，然后进行输出。结果的格式是<phrase, num>

``` java
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
```

此处实现了统计求和的功能。输出的结果的格式是<phrase,sum of the count>。同时，这里还有阈值的设置，只有词频大于输入的第一个参数k的时候，才会进行统计。

``` java
private static class IntWritableDecreasingComparator extends IntWritable.Comparator
```

此处实现了对结果的排序。








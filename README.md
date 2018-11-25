<h1>实验三实验报告</h1>
实验报告内容要求：
实验设计说明，包括主要设计思路、算法设计、程序和各个类的设计说明
程序运行和实验结果说明和分析
性能、扩展性等方面存在的不足和可能的改进之处
<h2> 需求一 </h2>
<h4>一、实验目的：</h4>
针对股票新闻数据集中的新闻标题，编写WordCount程序，统计所有除Stop-word（如“的”，“得”，“在”等）出现次数k次以上的单词计数，最后的结果按照词频从高到低排序输出。
<h4>二、实验的设计思路</h4>
包括两个mapreduce
此实验基于wordcount程序。首先进行分词，然后得到词频。然后，对这些键值对进行排序。
其中，调用了word.jar来实现了分词的功能。排序的功能采用了IntWritableDecreasingComparator.class以及InverseMapper.class（书上P118页）
<h4>三、算法设计以及各类分析</h4>

整体的流程：

* WordCountMapper

* IntSumReducer

* InverseMapper -> IntWritableDecreasingComparator

* reducer(这里是对输出的结果自动排序的，hadoop自动实现)

* **第一个类**
``` java 
public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>
```

此处实现了读取新闻的标题，然后进行输出。结果的格式是<phrase, num>

* **第二个类**
``` java
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
```

此处实现了统计求和的功能。输出的结果的格式是<phrase,sum of the count>。同时，这里还有阈值的设置，只有词频大于输入的第一个参数k的时候，才会进行统计。

* **第三个类**
``` java
private static class IntWritableDecreasingComparator extends IntWritable.Comparator
```

此处实现了对结果的排序。

<h4>四、改进以及不足之处</h4>

* 本来对于每一行的每一个字符进行判断（判断是否是中文字符，然后再进行分词和匹配）


>>> 我的改进方案 =>

* 之后，我对每一行取出，采用`line.split("\t")[5]` 首先分成多个数组，然后再取其中第五个（也就是中文的标题）。
* 但是值得注意的是有的行内容是有空缺的，所以我这里加了一个判断。


</hr>
</br>

<h2>需求二</h2>
<h4>一、实验目的：</h4>
针对股票新闻数据集，以新闻标题中的词组为key，编写带URL属性和词频的文档倒排索引程序，并按照词频从大到小排序，将结果输出到指定文件。输出格式可以如下：

高速公路， 10， 股票代码，[url0, url1,...,url9]

高速公路， 8， 股票代码，[url0, url1,...,url7]


<h4>二、实验的设计思路</h4>

包括两个mapreduce:

* 第一个mapreduce实现了将所有的键值对输出

* 第二个mapreduce实现了将所有的输出结果进行排序，以及对有所的词分词频进行排序


<h4>三、算法设计以及各类分析</h4>

整体的流程：

* wordCountMapper

* SortMapper

* **第一个类**

``` java 
public static class wordCountMapper extends Mapper<LongWritable,Text,Text,Text>
```

* 此处实现了取出股票的标题、股票的代码、股票的url。
* 接着对标题进行分词
* 输出的格式是<phrase + stock code, url, 1>
* 这里是1，是因为词都还没有合并

* **第二个类**

``` java
public static class wordCountReducer extends Reducer<Text,Text,Text,Text>
```

* 此处实现了对键值对的合并，然后统计总的出现的次数
* 输出的格式是<phrase + freq ， stock code + url >

* **第三个类**

``` java
public static class SortMapper extends Mapper<Text,Text,NewFormat,Text>
```

* 此处实现了对结果的排序。
* 同时，实验中发现，实验的text结果是无法直接拿过来排序的，所以定义了一个新的类来实现自动排序。

* **第四个类**

``` java
public class NewFormat implements WritableComparable<NewFormat>
```

* 在这里compare是可以按照（1）词（2）词频 进行升序排列的


<h4>四、改进以及不足之处</h4>

* 实验的过程中，发现两个文件的分隔符是不一样的，

>>> 我的改进方案 =>

* 所以进行了一次转换，然后再进行split的操作。


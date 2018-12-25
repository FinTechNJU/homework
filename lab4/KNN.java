import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;//这里没必要用reduce
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;

public class KNN{
    //map阶段计算出每条新闻最邻近的k个训练样本，输出<新闻，情感标签列表>
    //reduce阶段转化为情感输出<新闻，情感>

    public static int DISTANCE(int[] a,int[] b) {

        int dis = 0;
        for(int i = 0;i<a.length;i++) {
            dis += Math.abs(a[i]-b[i]);
        }
        return dis;

    }


    public static String[] GET_TRIAN_DATA(Configuration conf, Path inputpath) throws IOException{
        //获取训练集
        Text line = new Text();
        ArrayList<String> train = new ArrayList<String>();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsi = fs.open(inputpath);
        LineReader lr = new LineReader(fsi,conf);
        while(lr.readLine(line)>0) {
            train.add(line.toString());
        }
        lr.close();
        String[] b = (String[])train.toArray(new String[train.size()]);
        return b;
    }

    public static class KNNMapper extends Mapper<Text,Text,Text,Text>{


        private int[][] sample;
        private int[][] MATRIX;
        private int k,m,n;


        protected void setup(Context context) {
            //获取训练集和k
            String[] samples= context.getConfiguration().getStrings("sample");
            m = samples.length;

            n = samples[0].split(" ").length;
            MATRIX = new int[m][n+1];

            for(int i=0;i<m;i++) {
                String[] c = samples[i].split("\t");
                if(c.length==2){
                    String[] s = c[0].split(" ");
                    //这里一共有两个元素，第一个元素是数值，第二个元素是标签
                    for(int j=0;j<n;j++) {
                        MATRIX[i][j]=Integer.parseInt(s[j]);
                    }
                    MATRIX[i][n]=Integer.parseInt(c[1]);
                }
            }
            k = context.getConfiguration().getInt("k", 3);
        }

        public void map(Text key, Text value, Context context) throws IOException,InterruptedException{
            //新闻是键,向量是值.
            String[] MOOD = {"positive","negative","neutral"};

            int[] distances = new int[k];
            int[] TAG = new int[k];//情感标签，默认为0
            for(int i=0;i<k;i++) {
                distances[i]=Integer.MAX_VALUE;
            }

            String a=value.toString();
            String[] ARRAY=a.split(" ");

            int[] ints = new int[ARRAY.length];
            for(int i=0;i<ARRAY.length;i++){
                ints[i] = Integer.parseInt(ARRAY[i]);
            }


            int distance;
            for(int t=0;t<m;t++) {
                int[] s=MATRIX[t];

                distance = DISTANCE(ints,s);

                for(int i=0;i<k;i++) {
                    if(distance<distances[i]) {
                        distances[i]=distance;
                        TAG[i]=s[s.length-1];
                        break;
                    }
                }
            }
            String valueres = Integer.toString(TAG[0]);
            for(int j = 1; j<k;j++) {
                valueres += " "+Integer.toString(TAG[j]);
            }
            int predict=0;
            String[] lable = valueres.split(" ");
            int[] MATRIX = new int[3];
            for (int i = 0; i < lable.length; i++) {
                try {
                    MATRIX[Integer.parseInt(lable[i]) - 1] += 1;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (int j = 1; j < 3; j++) {
                if (MATRIX[j] > MATRIX[predict])
                    predict = j;
            }
            context.write(key, new Text(MOOD[predict]));

        }
    }




    public static void main(String[] args) throws Exception{
        //Usage: knn <train_in> <predict_in> <out>
        Configuration conf = new Configuration();
        Path inputPath1 = new Path(args[0]);//训练集
        Path inputPath2 = new Path(args[1]);//预测集
        Path outputPath = new Path(args[2]);
        String[] sample = GET_TRIAN_DATA(conf,inputPath1);
        conf.setStrings("sample", sample);

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath))
            fs.delete(outputPath,true);
        Job job = Job.getInstance(conf,"knnjob");
        job.setJarByClass(KNN.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(KNNMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, inputPath2);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}





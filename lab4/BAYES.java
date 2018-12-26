import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class BAYES{
    public static class TrainMapper extends Mapper<Text, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);

        public void map(Text key, Text value, Context context)throws IOException, InterruptedException {

            Text word = new Text();
            context.write(value, one);
            String[] vals = key.toString().split(" ");
            
            String line;
            for(int i=0;i<vals.length;i++) {
                line = value.toString()+" "+Integer.toString(i)+" "+vals[i];
                word.set(line);
                context.write(word, one);
            }
        }
    }

    public static class TrainReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable value = new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class TestMapper extends Reducer<Text,Text,Text,Text>
    {
        int Line;
        public void setup(Context context)
        {
            Configuration conf=context.getConfiguration();
            Line=context.getConfiguration().getInt("line");

        }
        public void map(Text key,Text value,Context context) throws IOException,InterruptedException
        {

            String str,vals[],temp;
            int i,j,k,fxyi,fyij,maxf,idx;
            Text id;
            Text cls;
            for(int t=0;i<Line;++i)
            {
                vals=str.split("");
                maxf=-100;
                idx=-1;
                for(i=0;i<num;++i){
                    fxyi=1;
                    String c1=temp;
                    Integer integer=Conf.className.get(c1);
                    if(iteger!=NULL){
                        fyi=0;
                    }
                    else{
                        fyi=integer.intValue();
                    }
                }
            }
            context.write(id.,cls);
        }

    }
    public static void main(String[] args) throws Exception{
        //Usage: BAYES <train_in> <predict_in> <out>
        Configuration conf = new Configuration();
        Path inputPath1 = new Path(args[0]);
        Path inputPath2 = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(inputPath2))
            fs.delete(inputPath2,true);
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(BAYES.class);
        job.setMapperClass(TrainMapper.class);
        job.setCombinerClass(TrainReducer.class);
        job.setReducerClass(TrainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath1);
        FileOutputFormat.setOutputPath(job, inputPath2);
        if(job.waitForCompletion(true)) {
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);
            Job testjob = Job.getInstance(conf, "job2");
            testjob.setJarByClass(BAYES.class);
            testjob.setMapperClass(TestMapper.class);
            testjob.setNumReduceTasks(1);
            testjob.setInputFormatClass(KeyValueTextInputFormat.class);
            testjob.setOutputKeyClass(Text.class);
            testjob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(testjob, inputPath1);
            FileOutputFormat.setOutputPath(testjob, outputPath);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

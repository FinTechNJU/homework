import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class WordCount{
    public static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable offset, Text value, Context context) throws IOException,InterruptedException{
            String line = value.toString();
            String []lines = line.split("\t");
            if(lines.length==6){
                line=line.split("\t")[4];
                List<Word> words = WordSegmenter.seg(line);
                for(Word w:words) {
                    word.set(w.getText());
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        private int k;
        protected void setup(Context context) {
            k = context.getConfiguration().getInt("k", 100);
        }
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            for (IntWritable val:values) {
                sum += val.get();
            }
            if(sum >= k) {
                result.set(sum);
                context.write(key,result);
            }
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator{
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1,l1,b2,s2,l2);
        }
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 3) {
            System.exit(2);
        }
        Configuration conf = new Configuration();
        conf.setInt("k", Integer.parseInt(args[0]));
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        Path tempPath = new Path(args[2]+"1");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(tempPath))
            fs.delete(tempPath, true);
        Job job = Job.getInstance(conf,"WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, tempPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        if(job.waitForCompletion(true)) {
            if(fs.exists(outputPath))
                fs.delete(outputPath,true);
            Job sortJob = Job.getInstance(conf,"sort");
            sortJob.setJarByClass(WordCount.class);
            sortJob.setMapperClass(InverseMapper.class);
            FileInputFormat.addInputPath(sortJob, tempPath);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
            sortJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(sortJob, outputPath);
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            System.exit(sortJob.waitForCompletion(true)?0:1);
        }
    }
}

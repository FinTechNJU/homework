import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.FileSystem;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class wordCount{

    public static class NewFormat implements WritableComparable<NewFormat> {
        //書上P264 定製自己的數據類型
        public String string;
        public int fre;

        public NewFormat() {}
        //构造函数
        public String toString() {
            return string+" "+Integer.toString(fre);
        }
        public void readFields(DataInput in) throws IOException{
            string = in.readUTF();fre = in.readInt();
        }
        public void write(DataOutput out) throws IOException{
            out.writeUTF(string);out.writeInt(fre);
        }

        public void set(String line) {
            String[] lines = line.split(" ");string = lines[0];fre = Integer.parseInt(lines[1]);
        }
        public int compareTo(NewFormat str) {
            int wordcompare = string.compareTo(str.string);
            int freq = str.fre-fre;
            if(wordcompare < 0)return 1;
            else if(wordcompare > 0)return -1;
            //首先根据string排序
            //当string相同的时候根据fre排序
            else return freq;
        }
    }

    public static class wordCountMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text key = new Text();
        private Text value = new Text();

        public void map(LongWritable offset,Text value,Context context) throws IOException,InterruptedException{

            String line = value.toString();
            String[] lines = line.split("\t");
            int length=lines.length;
            if(length - 6 == 0) {
                List<Word> words = WordSegmenter.seg(lines[4]);value.set(lines[5]+",1");
                for(Word i:words) {
                    key.set(i.getText()+","+lines[0]);
                    context.write(key, value);
                }
            }
            else
            {
                return;
            }
        }
    }

    public static class wordCountReducer extends Reducer<Text,Text,Text,Text>{
        private Text a = new Text();
        private Text b = new Text();
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            int sum = 0;String url = "";String[] lines = key.toString().split(",");
            for (Text i:values) {
                String[] tmp = i.toString().split(",");
                url = url + tmp[0]+" ";
                sum += Integer.parseInt(tmp[1]);
            }
            if(lines.length == 2) {
                a.set(lines[0]+" "+Integer.toString(sum));
                b.set(lines[1]+","+url);
                context.write(a, b);
            }
            else{
                return;
            }
        }
    }

    public static class SortMapper extends Mapper<Text,Text,NewFormat,Text>{
        //将key格式由Text转换为NewFormat
        private NewFormat newkey = new NewFormat();
        public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
            newkey.set(key.toString());
            context.write(newkey, value);
        }
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 2) {
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tempPath = new Path(args[1]+"1");
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(tempPath))
            fs.delete(tempPath,true);
        Job job = Job.getInstance(conf,"wordCount");
        job.setJarByClass(wordCount.class);
        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, tempPath);

        if(job.waitForCompletion(true)) {
            if(fs.exists(outputPath))
                fs.delete(outputPath,true);
            Job sortjob = Job.getInstance(conf,"sort");
            sortjob.setJarByClass(wordCount.class);
            sortjob.setInputFormatClass(KeyValueTextInputFormat.class);
            sortjob.setMapperClass(SortMapper.class);
            sortjob.setNumReduceTasks(1);
            sortjob.setOutputKeyClass(NewFormat.class);
            sortjob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(sortjob, tempPath);
            FileOutputFormat.setOutputPath(sortjob, outputPath);
            System.exit(sortjob.waitForCompletion(true)?0:1);
        }
    }
}

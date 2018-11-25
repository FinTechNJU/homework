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

    public static class NewText implements WritableComparable<NewText> {
    
        //書上P264 定製自己的數據類型
        public String string;
        public int fre;

        public NewText() {}
        //构造函数

        public void set(String line) {
            String[] linesplit = line.split(" ");
            string = linesplit[0];
            fre = Integer.parseInt(linesplit[1]);
        }

        public String toString() {
            return string+" "+Integer.toString(fre);
        }
        public void readFields(DataInput in) throws IOException{
            string = in.readUTF();
            fre = in.readInt();
        }

        public void write(DataOutput out) throws IOException{
            out.writeUTF(string);
            out.writeInt(fre);
        }

        public int compareTo(NewText str) {
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
        private Text wordkey = new Text();
        private Text wordvalue = new Text();

        public void map(LongWritable offset,Text value,Context context) throws IOException,InterruptedException{

            String line = value.toString();
            String[] linesplit = line.split("\t");

            if(linesplit.length == 6) {
                wordvalue.set(linesplit[5]+",1");
                List<Word> words = WordSegmenter.seg(linesplit[4]);
                for(Word w:words) {
                    wordkey.set(w.getText()+","+linesplit[0]);
                    context.write(wordkey, wordvalue);
                }
            }
            else
            {
                return;
            }
        }
    }

    public static class wordCountReducer extends Reducer<Text,Text,Text,Text>{
        private Text newkey = new Text();
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            int sum = 0;
            String urls = "";
            for (Text val:values) {
                String[] v = val.toString().split(",");
                sum += Integer.parseInt(v[1]);
                urls += v[0]+" ";
            }
            String[] k = key.toString().split(",");
            if(k.length == 2) {
                newkey.set(k[0]+" "+Integer.toString(sum));
                result.set(k[1]+","+urls);
                context.write(newkey, result);
            }
            else{
                return;
            }
        }
    }

    public static class SortMapper extends Mapper<Text,Text,NewText,Text>{
        //将key格式由Text转换为NewText
        private NewText newkey = new NewText();
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
            sortjob.setOutputKeyClass(NewText.class);
            sortjob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(sortjob, tempPath);
            FileOutputFormat.setOutputPath(sortjob, outputPath);
            System.exit(sortjob.waitForCompletion(true)?0:1);
        }
    }
}

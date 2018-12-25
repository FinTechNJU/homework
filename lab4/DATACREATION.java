import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.LineReader;

import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

public class DATACREATION{
    public static class DCMapper extends Mapper<Text,Text,Text,Text>{
        private String[] Merits;
        protected void setup(Context context) {
            Merits = context.getConfiguration().getStrings("Merits");
        }
        public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
            int[] vec = new int[Merits.length];
            List<Word> words = WordSegmenter.seg(key.toString());
            for(Word w:words) {
                String wo = w.getText();
                for(int i=0;i<Merits.length;i++) {
                    if(Merits[i].equals(wo)) {
                        vec[i]+=1;
                        break;
                    }
                }
            }

            String key1 = Integer.toString(vec[0]);

            for(int j = 1; j<Merits.length;j++) {
                key1 += " "+Integer.toString(vec[j]);
            }

            Text key2 = new Text();
            key2.set(key1);
            context.write(key2, value);
        }

    }

    public static class DC_Another_Mapper extends Mapper<LongWritable,Text,Text,Text>{
        private String[] Merits;//特征词

        protected void setup(Context context) {
            Merits = context.getConfiguration().getStrings("Merits");
        }
        public void map(LongWritable offset,Text value,Context context) throws IOException,InterruptedException{
            int[] vec = new int[Merits.length];

            String line = value.toString();
            String[] lines = line.split("\t");
            int length=lines.length;
            if(length == 6) {
                value.set(lines[5]+",1");
                List<Word> words = WordSegmenter.seg(lines[4]);
                for(Word j:words) {
                    String wo = j.getText();
                    for(int i=0;i<Merits.length;i++) {
                        if(Merits[i].equals(wo)) {
                            vec[i]+=1;
                            break;
                        }
                    }
                }
            }
            else
            {
                return;
            }


            String value1 = Integer.toString(vec[0]);
            for(int j = 1; j<Merits.length;j++) {
                String temp=Integer.toString(vec[j]);
                value1 += " "+temp;
            }
            Text key2 = new Text();
            Text value2 = new Text();
            key2.set(lines[0]+lines[4]);
            value2.set(value1);
            context.write(key2, value2);
        }

    }

    public static String[] getMerits(Configuration conf, Path inputpath) throws IOException{
        ArrayList<String> tzc = new ArrayList<String>();
        Text line = new Text();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsi = fs.open(inputpath);
        LineReader lr = new LineReader(fsi,conf);
        while(lr.readLine(line)>0) {
            tzc.add(line.toString());
        }
        lr.close();
        String[] b = (String[])tzc.toArray(new String[tzc.size()]);
        return b;
    }

    public static void main(String[] args) throws Exception{
        //Usage: DATACREATION <in dc> <in DC_Another_> <ci.txt> <out dc> <out DC_Another>
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path ciPath = new Path(args[2]);
        Path outputPath = new Path(args[3]);

        String[] tzc = getMerits(conf,ciPath);
        conf.setStrings("Merits", tzc);
        FileSystem fs = FileSystem.get(conf);

        if(fs.exists(outputPath))
            fs.delete(outputPath,true);

        Job job = Job.getInstance(conf,"create1job");
        job.setJarByClass(DATACREATION.class);
        job.setMapperClass(DCMapper.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true)?0:1);

        Path inputPath2 = new Path(args[1]);
        Path outputPath2 = new Path(args[4]);
        if(fs.exists(outputPath2))
            fs.delete(outputPath2,true);
        Job job2 = Job.getInstance(conf,"create2job");
        job2.setJarByClass(DataNewCreate.class);
        job2.setMapperClass(DC_Another_Mapper.class);
        job2.setNumReduceTasks(1);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, inputPath2);
        FileOutputFormat.setOutputPath(job2, outputPath2);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

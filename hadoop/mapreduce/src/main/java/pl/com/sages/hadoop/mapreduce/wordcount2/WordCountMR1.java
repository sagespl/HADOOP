package pl.com.sages.hadoop.mapreduce.wordcount2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by jsnow_000 on 2014-11-09.
 */
public class WordCountMR1 extends Configured implements Tool{
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                outputCollector.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            outputCollector.collect(key, result);
        }
    }

    public int run(String[] args) throws IOException {
        JobConf conf = new JobConf(getConf(), WordCountMR1.class);
        conf.setJobName("word count mr1");
        conf.setJarByClass(WordCountMR1.class);
        conf.setMapperClass(WordCountMR1.Map.class);
        conf.setReducerClass(WordCountMR1.Reduce.class);

        // Specify key / value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(conf, new Path(args[0]));

        // Output
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setOutputFormat(TextOutputFormat.class);

        // Execute job and return status
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountMR1(), args);
        System.exit(res);
    }
}

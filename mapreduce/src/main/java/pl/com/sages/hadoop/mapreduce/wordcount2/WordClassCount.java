package pl.com.sages.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 * Word Count
 */
public class WordClassCount extends Configured implements Tool {

    public enum WORD_CLASS {
        STARTS_WITH_A,
        NOT_STARTS_WITH_A
    }

    public static class Map extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());

            Counter startsWithA = context.getCounter(WORD_CLASS.STARTS_WITH_A);
            Counter notStartsWithA = context.getCounter(WORD_CLASS.NOT_STARTS_WITH_A);

            while (stringTokenizer.hasMoreTokens()) {
                String s = stringTokenizer.nextToken();

                if (s.toLowerCase().startsWith("a")) {
                    startsWithA.increment(1);
                }
                else {
                    notStartsWithA.increment(1);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "word class count");
        job.setJarByClass(WordClassCount.class);
        job.setMapperClass(WordClassCount.Map.class);

        // Specify key / value
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path("/user/hue/jobsub/sample_data/midsummer.txt"));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        // ... no output but something needs to be set
        String timeStamp = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        String outDir = String.format("%s/%s/%s", args[1], job.getJobName(), timeStamp);
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //Configuration conf = HadoopConfFactory.getConfiguration();
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new WordClassCount(), args);
        System.exit(res);
    }

}

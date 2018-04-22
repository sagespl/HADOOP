package pl.com.sages.hadoop.mapreduce.radek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RadekSearchRunner {

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        System.out.println(inputPath);

        Path outputPath = new Path(args[1]);
        System.out.println(outputPath);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);

        Job job = createJob(inputPath, outputPath, conf);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job createJob(final Path inputPath, final Path outputPath, final Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "search by Radek");
        job.setJarByClass(RadekSearchRunner.class);
        job.setMapperClass(RadekSearchMapper.class);
//        job.setCombinerClass(RadekSearchReducer.class);
        job.setReducerClass(RadekSearchReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}

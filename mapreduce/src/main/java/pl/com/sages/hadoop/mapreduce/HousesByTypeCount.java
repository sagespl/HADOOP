package pl.com.sages.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * House Count By Type
 */
public class HousesByTypeCount extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public static final int TYPE_COLUMN = 2;

        private Text houseType = new Text();
        private static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] dataLine = value.toString().split(",");
            houseType.set(dataLine[TYPE_COLUMN]);
            context.write(houseType, one);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce (Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = Job.getInstance(conf, "houses-by-type-count");
        job.setJarByClass(HousesByTypeCount.class);
        job.setMapperClass(HousesByTypeCount.Map.class);
        job.setReducerClass(HousesByTypeCount.Reduce.class);

        // Specify key / value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        String timeStamp = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
        String outDir = String.format("%s/%s/%s", args[1], job.getJobName(), timeStamp);
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HousesByTypeCount(), args);
        System.exit(res);
    }
}

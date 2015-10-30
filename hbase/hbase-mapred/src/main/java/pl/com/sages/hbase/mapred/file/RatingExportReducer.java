package pl.com.sages.hbase.mapred.file;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RatingExportReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {
            context.write(key, new DoubleWritable(sum / count));
        }
    }
}
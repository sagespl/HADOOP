package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AverageRatingReducer extends TableReducer<IntWritable, DoubleWritable, ImmutableBytesWritable> {

    public static final byte[] CF = "ratings_average".getBytes();
    public static final byte[] AVERAGE = "average".getBytes();

    public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double sum = 0;
        int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }

        if (count > 0) {

            Put put = new Put(Bytes.toBytes(key.get()));
            put.addColumn(CF, AVERAGE, Bytes.toBytes(sum / count));

            context.write(null, put);
        }
    }

}
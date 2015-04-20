package pl.com.sages.hbase.mapred.movies;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class AverageRatingMapper extends TableMapper<Text, DoubleWritable> {

    public static final byte[] CF = "ratings".getBytes();
    public static final byte[] ATTR1 = "movieId".getBytes();
    public static final byte[] ATTR2 = "rating".getBytes();

    private final DoubleWritable ONE = new DoubleWritable(1);
    private Text text = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        String movieId = new String(value.getValue(CF, ATTR1));
        text.set(movieId);

        double rating = Bytes.toDouble(value.getValue(CF, ATTR2));
        ONE.set(rating);

        context.write(text, ONE);
    }

}
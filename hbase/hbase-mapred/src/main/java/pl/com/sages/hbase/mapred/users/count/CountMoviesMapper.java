package pl.com.sages.hbase.mapred.users.count;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.com.sages.hbase.api.loader.LoadMovieData;

public class CountMoviesMapper extends TableMapper<Text, LongWritable> {

    public static enum Counters {
        MOVIE_COUNT
    }

    @Override
    protected void map(ImmutableBytesWritable rowkey, Result result, Mapper.Context context) {

        String genres = Bytes.toString(result.getValue(Bytes.toBytes(LoadMovieData.FAMILY_NAME), Bytes.toBytes(LoadMovieData.GENRES)));

        if (genres.toUpperCase().contains("Drama".toUpperCase())) {
            context.getCounter(Counters.MOVIE_COUNT).increment(1);
        }

    }

}

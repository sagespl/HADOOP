package pl.com.sages.hbase.mapred.users.count;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountUsersMapper extends TableMapper<Text, LongWritable> {

    public static enum Counters {
        USER_COUNT
    }

    @Override
    protected void map(ImmutableBytesWritable rowkey, Result result, Mapper.Context context) {

        context.getCounter(Counters.USER_COUNT).increment(1);

    }

}
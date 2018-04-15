package pl.com.sages.hbase.mapred.search;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.RadekBooksLoader;

import java.io.IOException;
import java.util.StringTokenizer;

public class RadekSearchMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static final String DELIMITERS = " \t\n\r\f,.:;![]()'*\"„”";

    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String filename = Bytes.toString(value.getValue(
                Bytes.toBytes(RadekBooksLoader.FAMILY_NAME),
                Bytes.toBytes("name")));
        String content = Bytes.toString(value.getValue(
                Bytes.toBytes(RadekBooksLoader.FAMILY_NAME),
                Bytes.toBytes("content")));

        StringTokenizer itr = new StringTokenizer(content, DELIMITERS);
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();

            Put put = new Put(Bytes.toBytes(word))
                    .addColumn(
                            Bytes.toBytes(RadekBooksLoader.FAMILY_NAME),
                            Bytes.toBytes(filename),
                            Bytes.toBytes(1));
            context.write(null, put);
        }
    }

}

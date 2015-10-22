package pl.com.sages.hbase.mapred.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import pl.com.sages.hbase.api.loader.LoadMovieData;

import java.io.IOException;

public class FilterMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
        Put put = resultToPut(row, value);
        if (put != null) {
            context.write(row, put);
        }
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {

        String movieId = Bytes.toString(result.getRow());
        String title = Bytes.toString(result.getValue(Bytes.toBytes(LoadMovieData.FAMILY_NAME), Bytes.toBytes(LoadMovieData.TITLE)));
        String generes = Bytes.toString(result.getValue(Bytes.toBytes(LoadMovieData.FAMILY_NAME), Bytes.toBytes(LoadMovieData.GENRES)));
        System.out.println(movieId + "::" + title + "::" + generes);

        if (generes.contains("|")) {
            Put put = new Put(key.get());
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                put.add(cell);
            }
            return put;
        }
        return null;
    }

}

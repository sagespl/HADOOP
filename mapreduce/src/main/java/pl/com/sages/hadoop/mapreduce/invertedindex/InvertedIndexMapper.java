package pl.com.sages.hadoop.mapreduce.invertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text index = new Text();
    private Text source = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();
        source.set(name);

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            index.set(itr.nextToken());
            context.write(index, source);
        }
    }

}

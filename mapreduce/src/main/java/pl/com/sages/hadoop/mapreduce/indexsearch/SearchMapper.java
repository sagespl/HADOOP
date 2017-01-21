package pl.com.sages.hadoop.mapreduce.indexsearch;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Created by xwsmolak on 1/21/17.
 */

public class SearchMapper extends Mapper<Object, Text, Text, Text>{

    public static final String DELIMITERS = " \t\n\r\f,.:;!?[]()'*\"„”";

    private Text fileName = new Text();
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String file = inputSplit.getPath().getName();
        fileName.set(file);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().trim(), DELIMITERS);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, fileName);
        }
    }
}

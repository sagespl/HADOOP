package pl.com.sages.hadoop.mapreduce.radek;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class RadekSearchMapper extends Mapper<Object, Text, Text, Text> {

    private static final String DELIMITERS = " \t\n\r\f,.:;![]()'*\"„”";

    private Text word = new Text();
    private Text file = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String filename = inputSplit.getPath().getName();
        file.set(filename);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().trim(), DELIMITERS);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());

            context.write(word, file);
        }
    }

}

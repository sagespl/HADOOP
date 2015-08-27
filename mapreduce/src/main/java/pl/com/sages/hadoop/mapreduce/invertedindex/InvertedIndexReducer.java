package pl.com.sages.hadoop.mapreduce.invertedindex;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> sources = new ArrayList<>();

        for (Text val : values) {
            sources.add(val.toString());
        }

        result.set(Joiner.on(",").join(sources));

        context.write(key, result);
    }

}
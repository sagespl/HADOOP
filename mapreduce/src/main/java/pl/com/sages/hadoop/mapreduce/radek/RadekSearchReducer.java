package pl.com.sages.hadoop.mapreduce.radek;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

// mapred job -list
// mapred job -kill job-id
public class RadekSearchReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Set<String> files = new HashSet<>();
        for (Text val : values) {
            files.add(val.toString());
        }

        String filesAsString = Joiner.on(", ").join(files);
        result.set(filesAsString);
        context.write(key, result);
    }

}
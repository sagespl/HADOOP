package pl.com.sages.hadoop.mapreduce.indexsearch;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by xwsmolak on 1/21/17.
 */
public class SearchReducer extends Reducer<Text, Text, Text, Text> {

    private Text result;



    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet files = new HashSet();
        for (Text value : values){
            files.add(value.toString());
        }
        result = new Text(Joiner.on(",").join(files));
        context.write(key,result);
    }


}

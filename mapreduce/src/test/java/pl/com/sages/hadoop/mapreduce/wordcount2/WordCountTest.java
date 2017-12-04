package pl.com.sages.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.wordcount2.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
    String text = "Some sentence with cat.\n Another cat.";

    @Before
    public void setUp() throws Exception {
        WordCount.Map mapper = new WordCount.Map();
        WordCount.Reduce reducer = new WordCount.Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(text));
        mapDriver.withOutput(new Text("Some"), new IntWritable(1));
        mapDriver.withOutput(new Text("sentence"), new IntWritable(1));
        mapDriver.withOutput(new Text("with"), new IntWritable(1));
        mapDriver.withOutput(new Text("cat."), new IntWritable(1));
        mapDriver.withOutput(new Text("Another"), new IntWritable(1));
        mapDriver.withOutput(new Text("cat."), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer () throws IOException {
        Text key = new Text("lala");
        List<IntWritable> values = new ArrayList<>();
        IntWritable sum = new IntWritable(6);
        for (int i=0; i < sum.get(); i++) {
            values.add(new IntWritable(1));
        }
        reduceDriver.withInput(key, values);
        reduceDriver.withOutput(key, sum);
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce () throws IOException {
        LongWritable inKey = new LongWritable(0);
        Text inValue = new Text(text);

        mapReduceDriver.withInput(inKey, inValue);
        mapReduceDriver.withOutput(new Text("Another"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("Some"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("cat."), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("sentence"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("with"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}
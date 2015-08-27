package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCountRunnerMRTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("Ala ma kota Ela ma psa"));
        mapDriver.withOutput(new Text("Ala"), new IntWritable(1));
        mapDriver.withOutput(new Text("ma"), new IntWritable(1));
        mapDriver.withOutput(new Text("kota"), new IntWritable(1));
        mapDriver.withOutput(new Text("Ela"), new IntWritable(1));
        mapDriver.withOutput(new Text("ma"), new IntWritable(1));
        mapDriver.withOutput(new Text("psa"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("ma"), values);
        reduceDriver.withOutput(new Text("ma"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("Ala ma kota Ela ma psa"));
        mapReduceDriver.withOutput(new Text("Ala"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("Ela"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("kota"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("ma"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("psa"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}
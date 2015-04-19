package pl.com.sages.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class WordClassCountTest {
    private MapDriver<LongWritable, Text, NullWritable, NullWritable> mapDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, LongWritable> mapReduceDriver;
    String sentence = "Some sentence with a word abnormal.";

    @Before
    public void setUp() throws Exception {
        WordClassCount.Map mapper = new WordClassCount.Map();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(sentence));
        // Don't do:
        //mapDriver.withOutput(NullWritable.get(), NullWritable.get());
        mapDriver.runTest();

        Counters counters = mapDriver.getCounters();
        assertEquals(counters.findCounter(WordClassCount.WORD_CLASS.STARTS_WITH_A).getValue(), 2);
        assertEquals(counters.findCounter(WordClassCount.WORD_CLASS.NOT_STARTS_WITH_A).getValue(), 4);
    }


}
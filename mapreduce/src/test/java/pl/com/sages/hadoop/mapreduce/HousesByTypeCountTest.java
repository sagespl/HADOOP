package pl.com.sages.hadoop.mapreduce;

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

public class HousesByTypeCountTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, LongWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, LongWritable> mapReduceDriver;
    String dataLine = "2,BATHGATE                 ,01  ONE FAMILY HOMES                        ,1,3028,25, ,A5, 412 EAST 179TH STREET                     ,            ,10457,1,0,1,1 842,2 048,1901,  1 , A5 ,$355 000,7/8/2013";
    String houseType = dataLine.split(",")[HousesByTypeCount.Map.TYPE_COLUMN];

    @Before
    public void setUp() throws Exception {
        HousesByTypeCount.Map mapper = new HousesByTypeCount.Map();
        HousesByTypeCount.Reduce reducer = new HousesByTypeCount.Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(dataLine));
        mapDriver.withOutput(new Text(houseType), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer () throws IOException {
        Text key = new Text(houseType);
        List<IntWritable> values = new ArrayList<>();
        LongWritable sum = new LongWritable(6);
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
        Text inValue = new Text(dataLine);

        Text key = new Text(houseType);
        LongWritable sum = new LongWritable(1);

        mapReduceDriver.withInput(inKey, inValue);
        mapReduceDriver.withOutput(key, sum);
        mapReduceDriver.runTest();
    }

}
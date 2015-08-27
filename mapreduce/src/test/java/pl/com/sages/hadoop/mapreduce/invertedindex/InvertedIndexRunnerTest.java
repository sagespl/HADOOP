package pl.com.sages.hadoop.mapreduce.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.PropertyReader;

import static org.junit.Assert.assertTrue;

public class InvertedIndexRunnerTest {

    private final String INPUT_PATH = PropertyReader.readProperty("pl.com.sages.hadoop.mapreduce.invertedindex.input");
    private final String OUTPUT_PATH = PropertyReader.readProperty("pl.com.sages.hadoop.mapreduce.invertedindex.output");

    @Test
    public void shouldGenerateInvertedIndex() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(OUTPUT_PATH), true);

        Job job = InvertedIndexRunner.createJob(new Path(INPUT_PATH), new Path(OUTPUT_PATH));

        //when
        job.waitForCompletion(true);

        //then
        assertTrue(fs.exists(new Path(OUTPUT_PATH + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(OUTPUT_PATH + "/part-r-00000")));
    }

}
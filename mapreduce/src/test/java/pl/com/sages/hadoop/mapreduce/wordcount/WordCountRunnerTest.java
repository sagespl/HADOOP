package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.PropertyReader;

import static org.junit.Assert.assertTrue;

public class WordCountRunnerTest {

    private final String INPUT_PATH = PropertyReader.readProperty("pl.com.sages.hadoop.mapreduce.wordcount.input");
    private final String OUTPUT_PATH = PropertyReader.readProperty("pl.com.sages.hadoop.mapreduce.wordcount.output");

    @Test
    public void shouldCountWords() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(OUTPUT_PATH), true);

        Job job = WordCountRunner.createJob(new Path(INPUT_PATH), new Path(OUTPUT_PATH));

        //when
        job.waitForCompletion(true);

        //then
        assertTrue(fs.exists(new Path(OUTPUT_PATH + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(OUTPUT_PATH + "/part-r-00000")));
    }

}
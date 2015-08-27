package pl.com.sages.hadoop.mapreduce.wordcount;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertTrue;

public class WordCountRunnerTest {

    @Rule
    public TemporaryFolder inputFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    private String inputPath;
    private String outputPath;

    @Before
    public void before() throws IOException {
        File tmpFile = inputFolder.newFile();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("20-000-mil-podmorskiej-zeglugi.txt");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        inputPath = inputFolder.getRoot().getAbsolutePath();
        outputPath = outputFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void shouldCountWords() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);

        Job job = WordCountRunner.createJob(new Path(inputPath), new Path(outputPath));

        //when
        job.waitForCompletion(true);

        //then
        assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(outputPath + "/part-r-00000")));
    }

}
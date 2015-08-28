package pl.com.sages.hadoop.mapreduce.movies;

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

public class MoviesTest {

    @Rule
    public TemporaryFolder inputFolder1 = new TemporaryFolder();

    @Rule
    public TemporaryFolder inputFolder2 = new TemporaryFolder();

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    private String inputPath1;
    private String inputPath2;
    private String outputPath;

    @Before
    public void before() throws IOException {
        File tmpFile = inputFolder1.newFile();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("movies.dat");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        tmpFile = inputFolder2.newFile();
        inputStream = getClass().getClassLoader().getResourceAsStream("tags.dat");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        inputPath1 = inputFolder1.getRoot().getAbsolutePath();
        inputPath2 = inputFolder2.getRoot().getAbsolutePath();
        outputPath = outputFolder.getRoot().getAbsolutePath();
    }

    @Test
    public void shouldCountWords() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);

        Job job = MoviesRunner.createJob(new Path(inputPath1), new Path(inputPath2), new Path(outputPath));

        //when
        job.waitForCompletion(true);

        //then
        assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(outputPath + "/part-r-00000")));
    }

}
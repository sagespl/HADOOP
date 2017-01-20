package pl.com.sages.hadoop.mapreduce.movielens;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by radek on 20.01.17.
 */
public class AbstractMovieLensTest {

    @Rule
    public TemporaryFolder moviesFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder tagsFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder ratingsFolder = new TemporaryFolder();

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    protected String moviesPath;
    protected String tagsPath;
    protected String ratingsPath;
    protected String outputPath;

    @Before
    public void before() throws IOException {
        File tmpFile = moviesFolder.newFile();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("movies.dat");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        tmpFile = tagsFolder.newFile();
        inputStream = getClass().getClassLoader().getResourceAsStream("tags.dat");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        tmpFile = ratingsFolder.newFile();
        inputStream = getClass().getClassLoader().getResourceAsStream("ratings.dat");
        IOUtils.copy(inputStream, new FileOutputStream(tmpFile));

        moviesPath = moviesFolder.getRoot().getAbsolutePath();
        tagsPath = tagsFolder.getRoot().getAbsolutePath();
        ratingsPath = ratingsFolder.getRoot().getAbsolutePath();
        outputPath = outputFolder.getRoot().getAbsolutePath();
    }

}

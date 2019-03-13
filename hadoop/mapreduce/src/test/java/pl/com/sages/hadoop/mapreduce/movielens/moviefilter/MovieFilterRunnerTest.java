package pl.com.sages.hadoop.mapreduce.movielens.moviefilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.movielens.AbstractMovieLensTest;

import static org.junit.Assert.assertTrue;

/**
 * Created by radek on 20.01.17.
 */
public class MovieFilterRunnerTest extends AbstractMovieLensTest {

    @Test
    public void shouldFilterMovies() throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);

        Job job = MovieFilterRunner.createJob(new Path(moviesPath), new Path(outputPath));

        //when
        boolean completion = job.waitForCompletion(true);

        //then
        assertTrue(completion);
        assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(outputPath + "/part-m-00000")));
    }

}
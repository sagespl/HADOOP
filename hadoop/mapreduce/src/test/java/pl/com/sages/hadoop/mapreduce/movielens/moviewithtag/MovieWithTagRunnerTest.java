package pl.com.sages.hadoop.mapreduce.movielens.moviewithtag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.movielens.AbstractMovieLensTest;

import static org.junit.Assert.assertTrue;

public class MovieWithTagRunnerTest extends AbstractMovieLensTest {

    @Test
    public void shouldJoinMoviesAndTags() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);

        Job job = MovieWithTagRunner.createJob(new Path(moviesPath), new Path(tagsPath), new Path(outputPath));

        //when
        boolean completion = job.waitForCompletion(true);

        //then
        assertTrue(completion);
        assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
        assertTrue(fs.exists(new Path(outputPath + "/part-r-00000")));
    }

}
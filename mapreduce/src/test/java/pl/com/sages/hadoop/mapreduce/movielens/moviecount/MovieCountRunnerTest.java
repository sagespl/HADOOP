package pl.com.sages.hadoop.mapreduce.movielens.moviecount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;
import pl.com.sages.hadoop.mapreduce.movielens.AbstractMovieLensTest;

import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by radek on 20.01.17.
 */
public class MovieCountRunnerTest extends AbstractMovieLensTest {

    @Test
    public void shouldCountMovies() throws Exception {
        //given
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);

        Job job = MovieCountRunner.createJob(new Path(moviesPath));

        //when
        boolean completion = job.waitForCompletion(true);

        //then
        assertTrue(completion);
        long value = job.getCounters().findCounter(MovieCounter.MOVIE_NUMBER).getValue();
        assertThat(value).isEqualTo(10681);
    }

}
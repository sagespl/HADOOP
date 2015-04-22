package pl.com.sages.hadoop.pig;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * PigUnit test
 */

public class PigUnitExampleTest {

    @Ignore
    @Test
    public void houseByTypeTest() throws IOException, ParseException {

        String[] args = {
                "in_file=../data/rollingsales_bronx.csv",
        };

        PigTest pigTest = new PigTest("scripts/houses_by_type.pig", args);

        String[] input = {
                "1, TYPE 1, lll",
                "1, TYPE 1, lll",
                "1, TYPE 2, lll",
                "1, TYPE 2, lll",
                "1, TYPE 2, lll"
        };

        String[] output = {
                "(TYPE 1,2)",
                "(TYPE 2,3)"
        };

        pigTest.assertOutput("raw", input, "count_by_type", output);
    }
}

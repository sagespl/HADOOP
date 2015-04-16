package pl.com.sages.hadoop.mapreduce.hive;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;

public class MD5HiveUdfTest {

    @Test
    public void testEvaluate() throws Exception {
        MD5HiveUdf udf = new MD5HiveUdf();
        Text inText = new Text("lala");
        Text outText = new Text("2e3817293fc275dbee74bd71ce6eb056");
        assertEquals(outText.toString(), udf.evaluate(inText).toString());
    }
}
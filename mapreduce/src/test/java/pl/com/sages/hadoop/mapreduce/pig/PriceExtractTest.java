package pl.com.sages.hadoop.mapreduce.pig;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import static org.junit.Assert.*;

public class PriceExtractTest {

    @Test
    public void testExec() throws Exception {
        PriceExtract utf = new PriceExtract();
        String priceIn = "$€£1000 000";
        Tuple inTuple = new DefaultTuple();
        inTuple.append(priceIn);

        Integer priceOut = new Integer (1000000);
        assertEquals(priceOut, utf.exec(inTuple));

        DataByteArray d = new DataByteArray();
        d.append(priceIn);
        inTuple = new DefaultTuple();
        inTuple.append(d);
        assertEquals(priceOut, utf.exec(inTuple));

    }
}
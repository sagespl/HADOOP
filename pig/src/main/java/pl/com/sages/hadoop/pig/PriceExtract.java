package pl.com.sages.hadoop.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Price Extract Pig UDF
 */
public class PriceExtract extends EvalFunc<Integer> {
    @Override
    public Integer exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0 || tuple.get(0) == null) {
            return null;
        }
        try {
            Object o = tuple.get(0);
            String price;
            if (o instanceof DataByteArray) {
                price = ((DataByteArray) o).toString();
            }
            else if (o instanceof String) {
                price = (String) o;
            }
            else {
                throw new IOException(String.format("Data of type %s", o.getClass().toString()));
            }
            return Integer.parseInt(price.replaceAll("[^\\d]", ""));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

package pl.com.sages.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Hive UDF example.
 *
 * @author Jakub Nowacki
 */
@Description (
        name="MD5HiveUDF",
        value="_FUNC()_ - computes MD5",
        extended = "Computes MD5 on text arguments"
)
public class MD5HiveUdf extends UDF {
    public Text evaluate(final Text s) {
        if (s == null) {
            return null;
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.toString().getBytes());
            byte[] md5hash = md.digest();
            StringBuilder builder = new StringBuilder();
            for (byte b : md5hash) {
                builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
            }
            return new Text(builder.toString());
        } catch (NoSuchAlgorithmException nsae) {
            System.out.println("Cannot find digest algorithm");
            System.exit(1);
        }
        return null;
    }
}

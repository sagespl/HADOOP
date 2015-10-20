package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;

/**
 * Created by btartanus on 30.11.14.
 *
 * Exercise 3
 • Create a DataModel
 • Feed the DataModel through a CSV file
 • Calculate similarities between users
 – CSV file should contain enough data!
 • Hints about objects to be used:
 – FileDataModel
 – PearsonCorrelationSimilarity,
 TanimotoCoefficientSimilarity, etc.

 */
public class Ex3 {
    public static void main(String[] args) throws Exception {
        DataModel model = new FileDataModel(new File("intro.csv"));
        UserSimilarity pearson = new PearsonCorrelationSimilarity(model);
        UserSimilarity euclidean = new EuclideanDistanceSimilarity(model);
        System.out.println("Pearson:"+pearson.userSimilarity(1, 2));
        System.out.println("Euclidean:"+euclidean.userSimilarity(1, 2));
        System.out.println("Pearson:"+pearson.userSimilarity(1, 3));
        System.out.println("Euclidean:"+euclidean.userSimilarity(1, 3));
    }

}

package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.List;

/**
 * Created by btartanus on 30.11.14.
 *
 *
 * Exercise 4
 • Create a DataModel
 • Feed the DataModel through a CSV file
 • Calculate similarities between users
 – CSV file should contain enough data!
 • Generate neighboorhood
 • Generate recommendations
 – Compare different combinations of parameters!
 • Hints about objects to be used:
 – NearestNUserNeighborhood
 – GenericUserBasedRecommender
 • Parameters: data model, neighborhood, similarity measure

 */
public class Ex4 {
    public static void main(String[] args) throws Exception {
        URL url = EvaluatorIntro.class.getClassLoader().getResource("intro.csv");
        URI uri = url.toURI();
        DataModel model = new FileDataModel(new File(uri));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(2, similarity, model);
        Recommender recommender = new GenericUserBasedRecommender(
                model, neighborhood, similarity);
        List<RecommendedItem> recommendations = recommender.recommend(1, 1);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }

}

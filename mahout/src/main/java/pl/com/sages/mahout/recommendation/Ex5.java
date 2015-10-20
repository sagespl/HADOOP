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
 * Exercise 5: MovieLens Recommender
 • Download the GroupLens dataset (100k)
 – Its format is already Mahout compliant
 – http://files.grouplens.org/datasets/movielens/ml-100k.zip
 • Preparatory Exercise: repeat exercise 3 (similarity
 calculations) with a bigger dataset
 • Next: now we can run the recommendation
 framework against a state-of-the-art dataset

 */
public class Ex5 {
    public static void main(String[] args) throws Exception {
        URL url = EvaluatorIntro.class.getClassLoader().getResource("ua.base");
        URI uri = url.toURI();
        DataModel model = new FileDataModel(new File(uri));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, model);
        Recommender recommender = new GenericUserBasedRecommender(
                model, neighborhood, similarity);
        List<RecommendedItem> recommendations = recommender.recommend(1, 20);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }

}

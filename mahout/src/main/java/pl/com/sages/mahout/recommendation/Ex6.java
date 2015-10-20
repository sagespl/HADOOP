package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * Created by btartanus on 30.11.14.
 *
 *
 * Exercise 6: Recommender Evaluation
 • Evaluate different CF recommender
 configurations on MovieLens data
 • Metrics: RMSE, MAE
 • Hints: useful classes
 – Implementations of RecommenderEvaluator
 interface
 • AverageAbsoluteDifferenceRecommenderEvaluator
 • RMSRecommenderEvaluator

 Further Hints:
 – Use RandomUtils.useTestSeed()to ensure
 the consistency among different evaluation runs
 – Invoke the evaluate() method
 • Parameters
 – RecommenderBuilder: recommender istance (as in previous
 exercises.
 – DataModelBuilder: specific criterion for training
 – Split Training-Test: double value (e.g. 0.7 for 70%)
 – Amount of data to use in the evaluation: double value (e.g 1.0 for
 100%)

 Mahout Strengths
 • Fast-prototyping and evaluation
 – To evaluate a different configuration of the same
 algorithm we just need to update a parameter and
 run again.
 – Example
 • Different Neighborhood Size


 */
public class Ex6 {
    public static void main(String[] args) throws Exception {
        RandomUtils.useTestSeed();
        URL url = EvaluatorIntro.class.getClassLoader().getResource("ua.base");
        URI uri = url.toURI();
        DataModel model = new FileDataModel(new File(uri));
        RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
        RecommenderEvaluator rmse = new RMSRecommenderEvaluator();

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(50, similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood, similarity);
            }
        };
        double scoreAAD = evaluator.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
        double scoreRMS = rmse.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
        System.out.println(scoreAAD);
        System.out.println(scoreRMS);
    }

}

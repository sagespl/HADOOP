package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
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

import static org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator.*;

/**
 * Created by btartanus on 30.11.14.
 *
 * Exercise 7: Recommender Evaluation
 • Evaluate different CF recommender
 configurations on MovieLens data
 • Metrics: Precision, Recall
 • Hints: useful classes
 – GenericRecommenderIRStatsEvaluator
 – Evaluate() method
 • Same parameters of exercise 6

 */
public class Ex7 {
    public static void main(String[] args) throws Exception {
        RandomUtils.useTestSeed();
        URL url = EvaluatorIntro.class.getClassLoader().getResource("ua.base");
        URI uri = url.toURI();
        DataModel model = new FileDataModel(new File(uri));
        RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
        // Build the same recommender for testing that we did last time:
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
                UserNeighborhood neighborhood = new NearestNUserNeighborhood(100, similarity, model);
                return new GenericUserBasedRecommender(model, neighborhood, similarity);
            }
        };
        IRStatistics stats = evaluator.evaluate(recommenderBuilder, null, model, null, 5, CHOOSE_THRESHOLD, 1);
        System.out.println(stats.getPrecision());
        System.out.println(stats.getRecall());
        System.out.println(stats.getF1Measure());
 }

}

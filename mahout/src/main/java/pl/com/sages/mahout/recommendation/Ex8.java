package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.RandomUtils;

import java.io.File;
import java.net.URI;
import java.net.URL;

/**
 * Created by btartanus on 30.11.14.
 *
 * Exercise 8: item-based recommender
 • Mahout provides Java classes for building an
 item-based recommender system
 – Amazon-like
 – Recommendations are based on similarities
 among items (generally pre-computed offline)
 – Evaluate it with the MovieLens dataset!

 *
 */
public class Ex8 {
    public static void main(String[] args) throws Exception {
        RandomUtils.useTestSeed();
        URL url = EvaluatorIntro.class.getClassLoader().getResource("u5.base");
        URI uri = url.toURI();
        DataModel model = new FileDataModel(new File(uri));
        RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                ItemSimilarity similarity = new PearsonCorrelationSimilarity(model);
                return new GenericItemBasedRecommender(model, similarity);
            }
        };
        IRStatistics stats = evaluator.evaluate(recommenderBuilder, null, model, null, 5,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
        System.out.println(stats.getPrecision());
        System.out.println(stats.getRecall());
        System.out.println(stats.getF1Measure());

    }

}

package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * Created by btartanus on 30.11.14.
 *
 * Exercise 2
 • Create a DataModel
 • Feed the DataModel through some simple
 Java calls
 • Print some statistics about data (how many
 users, how many items, maximum ratings,
 etc.)

 Hints about objects to be used:
 – FastByIdMap
 – Model

 PreferenceArray stores the preferences of a
 single user
 • Where do the preferences of all the users are
 stored?
 – An HashMap? No.
 – Mahout introduces data structures optimized for
 recommendation tasks
 • HashMap are replaced by FastByIDMap


 */
public class Ex2 {
    public static void main(String[] args) {
        FastByIDMap<PreferenceArray> preferences = new FastByIDMap<PreferenceArray>();
        PreferenceArray prefsForUser1 = new GenericUserPreferenceArray(10);
        prefsForUser1.setUserID(0, 1L);
        prefsForUser1.setItemID(0, 101L);
        prefsForUser1.setValue(0, 3.0f);
        prefsForUser1.setItemID(1, 102L);
        prefsForUser1.setValue(1, 4.5f);
        preferences.put(1L, prefsForUser1);
        DataModel model = new GenericDataModel(preferences);
        System.out.println(model);
    }

}

package pl.com.sages.mahout.recommendation;

import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

/**
 * Created by btartanus on 30.11.14.
 *
 * Exercise 1
 • Create a Preference object
 • Set preferences through some simple Java call
 • Print some statistics about preferences (how
 many preferences, on which items the user has
 expressed ratings, etc.)

 *Hints about objects to be used:
 – Preference
 – GenericUserPreferenceArray

 *
 */
public class Ex1 {
    public static void main(String[] args) {
        PreferenceArray user1Prefs = new GenericUserPreferenceArray(2);
        user1Prefs.setUserID(0, 1L);
        user1Prefs.setItemID(0, 101L);
        user1Prefs.setValue(0, 2.0f);
        user1Prefs.setItemID(1, 102L);
        user1Prefs.setValue(1, 3.0f);
        Preference pref = user1Prefs.get(1);
        System.out.println(pref);
    }

}

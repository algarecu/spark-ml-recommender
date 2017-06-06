package com.github.chen0040.sparkml.recommender;


import com.github.chen0040.sparkml.commons.SparkContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.Test;

import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
public class CFRecommenderUnitTest {

   @Test
   public void test_recommender(){
      JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");

      RatingTable ratingTable =new RatingTable();
      ratingTable.addRating("Love at last", "Alice", 5);
      ratingTable.addRating("Remance forever", "Alice", 5);
      ratingTable.addRating("Nonstop car chases", "Alice", 0);
      ratingTable.addRating("Sword vs. karate", "Alice", 0);
      ratingTable.addRating("Love at last", "Bob", 5);
      ratingTable.addRating("Cute puppies of love", "Bob", 4);
      ratingTable.addRating("Nonstop car chases", "Bob", 0);
      ratingTable.addRating("Sword vs. karate", "Bob", 0);
      ratingTable.addRating("Love at last", "Carol", 0);
      ratingTable.addRating("Cute puppies of love", "Carol", 0);
      ratingTable.addRating("Nonstop car chases", "Carol", 5);
      ratingTable.addRating("Sword vs. karate", "Carol", 5);
      ratingTable.addRating("Love at last", "Dave", 0);
      ratingTable.addRating("Remance forever", "Dave", 0);
      ratingTable.addRating("Nonstop car chases", "Dave", 4);

      JavaRDD<UserItemRating> input = context.parallelize(ratingTable.getRatings());

      CFRecommender recommender = new CFRecommender();
      recommender.setMaxIterations(50);
      recommender.setFeatureCount(2);

      JavaRDD<UserItemRating> output = recommender.fitAndTransform(input);

      List<UserItemRating> predicted = output.collect();
      for(UserItemRating cell : predicted){
         System.out.println("predict(" + cell.getItem() + ", " + cell.getUser() + "): " + cell.getValue());
      }
   }
}

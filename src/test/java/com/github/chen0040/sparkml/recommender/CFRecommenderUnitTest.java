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

      Table table =new Table();
      table.addRating("Love at last", "Alice", 5);
      table.addRating("Remance forever", "Alice", 5);
      table.addRating("Nonstop car chases", "Alice", 0);
      table.addRating("Sword vs. karate", "Alice", 0);
      table.addRating("Love at last", "Bob", 5);
      table.addRating("Cute puppies of love", "Bob", 4);
      table.addRating("Nonstop car chases", "Bob", 0);
      table.addRating("Sword vs. karate", "Bob", 0);
      table.addRating("Love at last", "Carol", 0);
      table.addRating("Cute puppies of love", "Carol", 0);
      table.addRating("Nonstop car chases", "Carol", 5);
      table.addRating("Sword vs. karate", "Carol", 5);
      table.addRating("Love at last", "Dave", 0);
      table.addRating("Remance forever", "Dave", 0);
      table.addRating("Nonstop car chases", "Dave", 4);

      JavaRDD<UserItemRating> input = context.parallelize(table.getRatings());

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

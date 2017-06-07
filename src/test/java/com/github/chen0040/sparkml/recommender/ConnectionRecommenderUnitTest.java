package com.github.chen0040.sparkml.recommender;


import com.github.chen0040.sparkml.commons.SparkContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.*;


/**
 * Created by xschen on 7/6/2017.
 */
public class ConnectionRecommenderUnitTest {

   @Test
   public void test(){
      List<Connection> connections = new ArrayList<>();
      connections.add(new Connection("Alice", Arrays.asList("Bob", "Dave"))); // Alice knows Bob and Dave
      connections.add(new Connection("Dave", Arrays.asList("Alice", "Carole")));
      connections.add(new Connection("Bob", Arrays.asList("James", "Alice", "Jim")));
      connections.add(new Connection("Jim", Arrays.asList("Bob", "Smith")));

      JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");
      JavaRDD<Connection> connectionJavaRDD = context.parallelize(connections);
      ConnectionRecommender recommender = new ConnectionRecommender();
      JavaRDD<ConnectionRecommendation> recommendationJavaRDD = recommender.fitAndTransform(connectionJavaRDD);

      List<ConnectionRecommendation> recommendations = recommendationJavaRDD.collect();

      for(ConnectionRecommendation recommendation : recommendations){
         System.out.println(recommendation.getPerson1() + " can be connected to " + recommendation.getPerson2() + " via " + recommendation.getCommonFriends());
      }
   }
}

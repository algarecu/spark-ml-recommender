package com.github.chen0040.sparkml.recommender;


import com.github.chen0040.sparkml.commons.SparkContextFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.Test;

import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
public class ItemCorrelationRecommenderUnitTest {

   @Test
   public void test(){
      JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");

      Table table =new Table();
      table.addCell("Love at last", "Alice", 5);
      table.addCell("Remance forever", "Alice", 5);
      table.addCell("Nonstop car chases", "Alice", 0);
      table.addCell("Sword vs. karate", "Alice", 0);
      table.addCell("Love at last", "Bob", 5);
      table.addCell("Cute puppies of love", "Bob", 4);
      table.addCell("Nonstop car chases", "Bob", 0);
      table.addCell("Sword vs. karate", "Bob", 0);
      table.addCell("Love at last", "Carol", 0);
      table.addCell("Cute puppies of love", "Carol", 0);
      table.addCell("Nonstop car chases", "Carol", 5);
      table.addCell("Sword vs. karate", "Carol", 5);
      table.addCell("Love at last", "Dave", 0);
      table.addCell("Remance forever", "Dave", 0);
      table.addCell("Nonstop car chases", "Dave", 4);

      JavaRDD<TableCell> input = context.parallelize(table.getCells());

      ItemCorrelationRecommender recommender = new ItemCorrelationRecommender();

      JavaRDD<ItemCorrelation> output = recommender.fitAndTransform(input);

      List<ItemCorrelation> predicted = output.collect();
      for(ItemCorrelation cell : predicted){
         System.out.println("predict(" + cell.getItem1() + ", " + cell.getItem2() + "): " + cell.getPearson());
      }
   }
}
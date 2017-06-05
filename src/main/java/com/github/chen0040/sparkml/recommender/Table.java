package com.github.chen0040.sparkml.recommender;


import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;


/**
 * Created by xschen on 5/6/2017.
 */
public class Table {
   private JavaRDD<TableCell> cells;

   public Table(JavaRDD<TableCell> cells) {
      this.cells = cells;
   }
}

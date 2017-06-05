package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import scala.Tuple2;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
public class Table {
   private List<TableCell> cells  = new ArrayList<>();

   public Table() {

   }

   public void addCell(String rowName, String columnName, double value) {
      TableCell cell =new TableCell();
      cell.setRowName(rowName);
      cell.setColumnName(columnName);
      cell.setValue(value);
      cells.add(cell);
   }


}

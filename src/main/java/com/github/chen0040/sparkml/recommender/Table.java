package com.github.chen0040.sparkml.recommender;


import lombok.Getter;

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

   public void addCell(String item, String user, double value) {
      TableCell cell =new TableCell();
      cell.setItem(item);
      cell.setUser(user);
      cell.setValue(value);
      cells.add(cell);
   }


}

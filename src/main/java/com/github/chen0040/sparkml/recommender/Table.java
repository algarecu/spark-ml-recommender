package com.github.chen0040.sparkml.recommender;


import lombok.Getter;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
public class Table {
   private List<UserItemRating> cells  = new ArrayList<>();

   public Table() {

   }

   public void addRating(String item, String user, double value) {
      UserItemRating cell =new UserItemRating();
      cell.setItem(item);
      cell.setUser(user);
      cell.setValue(value);
      cells.add(cell);
   }


}

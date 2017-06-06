package com.github.chen0040.sparkml.recommender;


import lombok.Getter;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
public class RatingTable {
   private List<UserItemRating> ratings = new ArrayList<>();

   public RatingTable() {

   }

   public void addRating(String item, String user, double value) {
      UserItemRating cell =new UserItemRating();
      cell.setItem(item);
      cell.setUser(user);
      cell.setValue(value);
      ratings.add(cell);
   }


}

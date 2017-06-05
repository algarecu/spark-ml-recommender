package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
@Setter
public class Theta implements Serializable {
   private static final long serialVersionUID = 5804571482633027589L;
   private List<Double> values = new ArrayList<>();
   private String columnName;

   public double get(int index) {
      return values.get(index);
   }


   public int size() {
      return values.size();
   }
}

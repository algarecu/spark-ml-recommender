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
public class X implements Serializable {
   private static final long serialVersionUID = -2453992460883398551L;

   private List<Double> values = new ArrayList<>();

   private String rowName;


   public double dotProduct(Theta theta) {
      double h = 0;
      for(int k=0; k < values.size(); ++k){
         h += theta.getValues().get(k) * values.get(k);
      }
      return h;
   }

   public double get(int index){
      return values.get(index);
   }


   public int size() {
      return values.size();
   }
}

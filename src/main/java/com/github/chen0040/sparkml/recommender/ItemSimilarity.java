package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
@Setter
public class ItemSimilarity {
   private double pearson;
   private double jaccard;
   private double cosine;

   private String item1;
   private String item2;
}

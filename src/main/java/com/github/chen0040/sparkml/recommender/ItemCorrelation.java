package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
@Setter
public class ItemCorrelation implements Serializable {
   private static final long serialVersionUID = -251924475410494096L;
   private double pearson;
   private double jaccard;
   private double cosine;

   private String item1;
   private String item2;
}

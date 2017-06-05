package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
@Setter
public class TableCell implements Serializable {
   private static final long serialVersionUID = 8246834031829454204L;

   private String rowName = "";
   private String columnName = "";

   private double value;
}

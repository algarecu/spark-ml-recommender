package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;


/**
 * Created by xschen on 5/6/2017.
 */
@Getter
@Setter
public class UserItemRating implements Serializable {
   private static final long serialVersionUID = 8246834031829454204L;

   private String item = "";
   private String user = "";

   private double value;
}

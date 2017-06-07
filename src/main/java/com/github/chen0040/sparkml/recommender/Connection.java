package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 6/6/2017.
 */
@Getter
@Setter
public class Connection implements Serializable {
   private String person;
   private List<String> hisFriends = new ArrayList<>();

   public Connection(){

   }

   public Connection(String person, List<String> friends) {
      this.person = person;
      this.hisFriends = friends;
   }

}

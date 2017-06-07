package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;


/**
 * Created by xschen on 6/6/2017.
 */
@Getter
@Setter
public class ConnectionRecommendation {
   private String person1;
   private String person2;
   private Set<String> commonFriends = new HashSet<>();
}

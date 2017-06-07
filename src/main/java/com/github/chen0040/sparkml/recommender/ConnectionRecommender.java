package com.github.chen0040.sparkml.recommender;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.*;


/**
 * Created by xschen on 22/2/2016.
 */
public class ConnectionRecommender {

   public JavaRDD<ConnectionRecommendation> fitAndTransform(JavaRDD<Connection> rdd) {


      JavaPairRDD<String, Tuple2<String, String>> rdd1 = rdd.flatMapToPair( s -> {

         String person = s.getPerson();
         List<String> friendList = s.getHisFriends();
         List<Tuple2<String, Tuple2<String, String>>> result = new ArrayList<>();

         for (int i = 0; i < friendList.size(); ++i) {
            result.add(new Tuple2<>(person, new Tuple2<>(friendList.get(i), "*")));
         }

         for (int i = 0; i < friendList.size() - 1; ++i) {
            for (int j = i + 1; j < friendList.size(); ++j) {
               result.add(new Tuple2<>(friendList.get(i), new Tuple2<>(friendList.get(j), person)));
               result.add(new Tuple2<>(friendList.get(j), new Tuple2<>(friendList.get(i), person)));
            }
         }

         return result;
      });

      JavaPairRDD<String, Map<String, Set<String>>> rdd2 = rdd1.combineByKey((Function<Tuple2<String, String>, Map<String, Set<String>>>) s1 -> {
         String toUser = s1._1();
         String commonFriend = s1._2();
         Map<String, Set<String>> map = new HashMap<>();

         if (commonFriend.equals("*")) {
            map.put(toUser, null);
         }
         else {
            Set<String> set = new HashSet<>();
            set.add(commonFriend);
            map.put(toUser, set);
         }
         return map;
      }, (Function2<Map<String, Set<String>>, Tuple2<String, String>, Map<String, Set<String>>>) (map1, s2) -> {
         String toUser = s2._1();
         String commonFriend = s2._2();

         boolean alreadyFriend = commonFriend.equals("*");

         if (map1.containsKey(toUser)) {
            if (alreadyFriend) {
               map1.put(toUser, null);
            }
            else {
               if (map1.get(toUser) != null) {
                  map1.get(toUser).add(commonFriend);
               }
            }
         }
         else {
            if (alreadyFriend) {
               map1.put(toUser, null);
            }
            else {
               Set<String> set = new HashSet<>();
               set.add(commonFriend);
               map1.put(toUser, set);
            }
         }

         return map1;

      }, (Function2<Map<String, Set<String>>, Map<String, Set<String>>, Map<String, Set<String>>>) (map1, map2) -> {
         for (Map.Entry<String, Set<String>> entry2 : map2.entrySet()) {
            String toUser = entry2.getKey();
            Set<String> set2 = entry2.getValue();

            if (set2 == null) {
               map1.put(toUser, null);
            }
            else {
               if (map1.containsKey(toUser)) {
                  if (map1.get(toUser) != null) {
                     map1.get(toUser).addAll(set2);
                  }
               }
               else {
                  map1.put(toUser, set2);
               }
            }
         }
         return map1;
      });

      return rdd2.flatMap(tuple2 -> {
         String person1 = tuple2._1();
         Map<String, Set<String>> links = tuple2._2();

         List<ConnectionRecommendation> result = new ArrayList<>();
         for(Map.Entry<String, Set<String>> entry : links.entrySet()){
            String person2 = entry.getKey();
            Set<String> commonFriends = entry.getValue();
            ConnectionRecommendation link = new ConnectionRecommendation();
            link.setPerson1(person1);
            link.setPerson2(person2);
            link.setCommonFriends(commonFriends);
            result.add(link);
         }
         return result;
      }).filter(r -> r.getCommonFriends() != null);
   }
}

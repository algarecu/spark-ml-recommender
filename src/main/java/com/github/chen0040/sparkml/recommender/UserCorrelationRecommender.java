package com.github.chen0040.sparkml.recommender;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by xschen on 5/6/2017.
 */
public class UserCorrelationRecommender {

   public JavaRDD<UserCorrelation> fitAndTransform(JavaRDD<UserItemRating> cells){
      JavaPairRDD<String, Tuple2<String, Double>> rdd2 = cells.mapToPair(cell -> {

            String user = cell.getUser();
            String movie = cell.getItem();
            double rating = cell.getValue();
            return new Tuple2<>(movie, new Tuple2<>(user, rating));
         });

      JavaPairRDD<String, Iterable<Tuple2<String, Double>>> rdd3 = rdd2.groupByKey();

      JavaPairRDD<String, Iterable<Tuple3<String, Double, Integer>>> rdd4 = rdd3.mapValues(s -> {
            List<Tuple2<String, Double>> values = new ArrayList<>();
            for(Tuple2<String, Double> entry : s) {
               values.add(entry);
            }

            int count = values.size(); // number of ratings given to a movie
            List<Tuple3<String, Double, Integer>> result = new ArrayList<>();
            for(Tuple2<String, Double> entry : values) {
               result.add(new Tuple3<>(entry._1(), entry._2(), count));
            }

            return result;
         });

      JavaPairRDD<Tuple2<String, String>, Tuple7<Double, Double, Integer, Integer, Double, Double, Double>> rdd5 = rdd4.flatMapToPair(s -> {
            String movie = s._1();
            Iterable<Tuple3<String, Double, Integer>> movie_ratings = s._2();
            List<Tuple3<String, Double, Integer>> values = new ArrayList<>();
            for(Tuple3<String, Double, Integer> entry : movie_ratings) {
               values.add(entry);
            }

            List<Tuple2<Tuple2<String, String>, Tuple7<Double, Double, Integer, Integer, Double, Double, Double>>> result = new ArrayList<>();

            for(int i=0; i < values.size()-1; ++i) {
               Tuple3<String, Double, Integer> movie_rating_i = values.get(i);
               String user1 = movie_rating_i._1();
               double rating1 = movie_rating_i._2();
               int numRater1 = movie_rating_i._3();

               for(int j=i+1; j < values.size(); ++j) {
                  Tuple3<String, Double, Integer> movie_rating_j = values.get(j);
                  String user2 = movie_rating_j._1();
                  double rating2 = movie_rating_j._2();
                  int numRater2 = movie_rating_j._3();

                  double ratingProd = rating1 * rating2;
                  double ratingSqr1 = rating1 * rating1;
                  double ratingSqr2 = rating2 * rating2;

                  result.add(new Tuple2<>(new Tuple2<>(user1, user2),
                          new Tuple7<>(rating1, rating2, numRater1, numRater2, ratingProd, ratingSqr1, ratingSqr2)));
               }
            }

            return result;

         });

      JavaPairRDD<Tuple2<String, String>, Iterable<Tuple7<Double, Double, Integer, Integer, Double,Double, Double>>> rdd6 = rdd5.groupByKey();

      JavaPairRDD<Tuple2<String, String>, Iterable<Tuple7<Double, Double, Integer, Integer, Double,Double, Double>>> rdd7 = rdd6.filter(s -> {
            Tuple2<String, String> moviePair = s._1();
            return moviePair._1().compareTo(moviePair._2()) < 0;
         });

      return rdd7.map(t -> {

         Iterable<Tuple7<Double, Double, Integer, Integer, Double, Double, Double>> s = t._2();
         Tuple2<String, String> movie_pair = t._1();
            double sumRating1 = 0;
            double sumRating2 = 0;
            double sumSqrRating1 = 0;
            double sumSqrRating2 = 0;
            double sumRatingProd = 0;

            int N = 0;
            int maxNumRaters1 = 0;
            int maxNumRaters2 = 0;
            for(Tuple7<Double, Double, Integer, Integer, Double, Double, Double> entry : s) {
               sumRating1 += entry._1();
               sumRating2 += entry._2();
               maxNumRaters1 = Math.max(maxNumRaters1, entry._3());
               maxNumRaters2 = Math.max(maxNumRaters2, entry._4());

               sumRatingProd += entry._5();

               sumSqrRating1 += entry._6();
               sumSqrRating2 += entry._7();
               N++;
            }



            double pearson = (N * sumRatingProd - sumRating1 * sumRating2) / (Math.sqrt(N * sumSqrRating1 - sumRating1 * sumRating1) * Math.sqrt(N * sumSqrRating2 - sumRating2 * sumRating2));
            double jaccard = (double)N / (maxNumRaters1 + maxNumRaters2 - N);
            double cosine = sumRatingProd / (Math.sqrt(sumSqrRating1) * Math.sqrt(sumSqrRating2));

            UserCorrelation result = new UserCorrelation();
            result.setCosine(cosine);
            result.setPearson(pearson);
            result.setJaccard(jaccard);
            result.setUser1(movie_pair._1());
            result.setUser2(movie_pair._2());

            return result;

         });


   }
}

package com.github.chen0040.sparkml.recommender;


import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * Created by xschen on 5/6/2017.
 * content collaborative filtering recommend-er using ALS (Alternating Least Square) stochastic gradient descent
 */
@Getter
@Setter
public class CFRecommender {

   private int maxIterations;
   private double alpha = 0.02;
   private double lambda = 0.0;
   private Integer featureCount = 2;
   private int partitionCount  = 100;

   public JavaRDD<TableCell> fitAndTransform(JavaRDD<TableCell> cells){
      JavaSparkContext context = JavaSparkContext.fromSparkContext(cells.context());

      Broadcast<Double> alphaBroadcast = context.broadcast(alpha);
      Broadcast<Double> lambdaBroadcast = context.broadcast(lambda);
      Broadcast<Integer> featureCountBroadcast = context.broadcast(featureCount);

      JavaPairRDD<String, Theta> thetaRdd = cells.map(TableCell::getColumnName).distinct().mapToPair(columnName -> {
         int fc = featureCountBroadcast.value();
         Theta theta = new Theta();
         Random random = new Random();

         List<Double> valuesTheta = new ArrayList<>();
         for(int i=0; i < fc; ++i){
            valuesTheta.add(random.nextDouble());
         }

         theta.setValues(valuesTheta);
         theta.setColumnName(columnName);
         return new Tuple2<>(columnName, theta);
      }).coalesce(partitionCount).cache();

      JavaPairRDD<String, X> xRdd = cells.map(TableCell::getRowName).distinct().mapToPair(rowName -> {
         int fc = featureCountBroadcast.value();
         X x = new X();
         x.setRowName(rowName);
         Random random = new Random();

         List<Double> valuesX  = new ArrayList<>();
         for(int i=0; i < fc; ++i){
            valuesX.add(random.nextDouble());
         }
         x.setValues(valuesX);

         return new Tuple2<>(rowName, x);
      }).coalesce(partitionCount).cache();

      JavaPairRDD<String, TableCell> rowRadd = cells.mapToPair(cell -> new Tuple2<>(cell.getRowName(), cell)).coalesce(partitionCount).cache();
      JavaPairRDD<String, TableCell> colRadd = cells.mapToPair(cell -> new Tuple2<>(cell.getColumnName(), cell)).coalesce(partitionCount).cache();

      for(int iterations=0; iterations < maxIterations; ++iterations){

         JavaPairRDD<String, Tuple2<TableCell, X>> rdd1 = rowRadd.leftOuterJoin(xRdd).mapToPair(tuple2 -> {
            TableCell cell = tuple2._2()._1();
            X x = tuple2._2()._2().get();
            String columnName = cell.getColumnName();
            return new Tuple2<>(columnName, new Tuple2<>(cell, x));
         });

         // theta least square
         thetaRdd = rdd1.leftOuterJoin(thetaRdd).mapValues(a -> {
            Theta theta = a._2().get();
            TableCell cell = a._1()._1();
            X x = a._1()._2();
            double h = x.dotProduct(theta);
            double[] dTheta = new double[theta.size()];
            double dy = h - cell.getValue();

            for(int k = 0; k < dTheta.length; ++k) {
               dTheta[k] = dy * x.get(k);
            }

            return new Tuple2<>(theta, dTheta);
         }).reduceByKey((a, b) -> {
            double[] dTheta1 = a._2();
            double[] dTheta2 = b._2();
            double[] dTheta = new double[dTheta1.length];
            for(int k =0; k < dTheta.length; ++k) {
               dTheta[k] = dTheta1[k] + dTheta2[k];
            }
            return new Tuple2<>(a._1(), dTheta);
         }).mapValues(a -> {
            double _lambda = lambdaBroadcast.getValue();
            double _alpha = alphaBroadcast.getValue();

            Theta theta = a._1();
            double[] dTheta = a._2();
            for(int k=0; k < dTheta.length; ++k) {
               dTheta[k] += _lambda * theta.get(k);
            }
            List<Double> thetaValues = new ArrayList<>();
            for(int k=0; k < dTheta.length; ++k) {
               thetaValues.add(theta.getValues().get(k) - _alpha * dTheta[k]);
            }
            theta.setValues(thetaValues);
            return theta;
         }).cache();

         thetaRdd.count();

         JavaPairRDD<String, Tuple2<TableCell, Theta>> rdd2 = colRadd.leftOuterJoin(thetaRdd).mapToPair(tuple2 ->{
            TableCell cell = tuple2._2()._1();
            Theta theta = tuple2._2()._2().get();
            String columnName = cell.getRowName();
            return new Tuple2<>(columnName, new Tuple2<>(cell, theta));
         });

         // theta least square
         xRdd = rdd2.leftOuterJoin(xRdd).mapValues(a -> {
            X x = a._2().get();
            TableCell cell = a._1()._1();
            Theta theta = a._1()._2();
            double h = x.dotProduct(theta);
            double[] dX = new double[x.size()];

            double dy = h - cell.getValue();

            for(int k = 0; k < dX.length; ++k) {
               dX[k] = dy * theta.get(k);
            }

            return new Tuple2<>(x, dX);
         }).reduceByKey((a, b) -> {
            double[] dX1 = a._2();
            double[] dX2 = b._2();
            double[] dX = new double[dX1.length];
            for(int k =0; k < dX.length; ++k) {
               dX[k] = dX1[k] + dX2[k];
            }
            return new Tuple2<>(a._1(), dX);
         }).mapValues(a -> {
            double _lambda = lambdaBroadcast.getValue();
            double _alpha = alphaBroadcast.getValue();

            X x = a._1();
            double[] dX = a._2();
            for(int k=0; k < dX.length; ++k) {
               dX[k] += _lambda * x.getValues().get(k);
            }
            List<Double> xValues = new ArrayList<>();
            for(int k=0; k < dX.length; ++k) {
               xValues.add(x.getValues().get(k) - _alpha * dX[k]);
            }
            x.setValues(xValues);
            return x;
         }).cache();

         xRdd.count();
      }

      return thetaRdd.values().cartesian(xRdd.values()).map(tuple2 -> {
         Theta theta = tuple2._1();
         X x = tuple2._2();
         double predicted = x.dotProduct(theta);
         TableCell cell = new TableCell();
         cell.setColumnName(theta.getColumnName());
         cell.setRowName(x.getRowName());
         cell.setValue(predicted);
         return cell;
      });
   }

}

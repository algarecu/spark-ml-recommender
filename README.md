# spark-ml-recommender

Package provides java implementation of big-data recommend-er using Apache Spark

# Feature

* Collaborative filtering recommender that predicts user's preference on unknown items using ALS-based gradient descent
* Recommender that computes the correlation / similarity between items based on user preference using Pearson / Cosine / Jaccard correlation coefficient
* Recommender that recommends a common friend to connect two persons who do not know each other but like to know each other (something similar to linkedIn connection)

# Install

Add the following dependency to your POM file:

```xml
<dependency>
  <groupId>com.github.chen0040</groupId>
  <artifactId>spark-ml-recommender</artifactId>
  <version>1.0.4</version>
</dependency>
```

# Usage

## Predict missing ratings 

The sample code below tries to predict the missing rating of [user, movie] as shown in the ratingTable below:

![movie-recommender](images/movie-recommender.png)

```java
JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");

RatingTable ratingTable =new RatingTable();
ratingTable.addRating("Love at last", "Alice", 5);
ratingTable.addRating("Remance forever", "Alice", 5);
ratingTable.addRating("Nonstop car chases", "Alice", 0);
ratingTable.addRating("Sword vs. karate", "Alice", 0);
ratingTable.addRating("Love at last", "Bob", 5);
ratingTable.addRating("Cute puppies of love", "Bob", 4);
ratingTable.addRating("Nonstop car chases", "Bob", 0);
ratingTable.addRating("Sword vs. karate", "Bob", 0);
ratingTable.addRating("Love at last", "Carol", 0);
ratingTable.addRating("Cute puppies of love", "Carol", 0);
ratingTable.addRating("Nonstop car chases", "Carol", 5);
ratingTable.addRating("Sword vs. karate", "Carol", 5);
ratingTable.addRating("Love at last", "Dave", 0);
ratingTable.addRating("Remance forever", "Dave", 0);
ratingTable.addRating("Nonstop car chases", "Dave", 4);

JavaRDD<UserItemRating> input = context.parallelize(ratingTable.getRatings());

CFRecommender recommender = new CFRecommender();
recommender.setMaxIterations(50);
recommender.setFeatureCount(2);

JavaRDD<UserItemRating> output = recommender.fitAndTransform(input);

List<UserItemRating> predicted = output.collect();
for(UserItemRating cell : predicted){
 System.out.println("predict(" + cell.getItem() + ", " + cell.getUser() + "): " + cell.getValue());
}
```

## To find the correlation between any two movies using user rating:

The sample code below tries to find the correlation between items rated by users. Can be used for scenario such as "Customer who bought this also bought"

```java
JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");

RatingTable ratingTable =new RatingTable();
ratingTable.addRating("Love at last", "Alice", 5);
ratingTable.addRating("Remance forever", "Alice", 5);
ratingTable.addRating("Nonstop car chases", "Alice", 0);
ratingTable.addRating("Sword vs. karate", "Alice", 0);
ratingTable.addRating("Love at last", "Bob", 5);
ratingTable.addRating("Cute puppies of love", "Bob", 4);
ratingTable.addRating("Nonstop car chases", "Bob", 0);
ratingTable.addRating("Sword vs. karate", "Bob", 0);
ratingTable.addRating("Love at last", "Carol", 0);
ratingTable.addRating("Cute puppies of love", "Carol", 0);
ratingTable.addRating("Nonstop car chases", "Carol", 5);
ratingTable.addRating("Sword vs. karate", "Carol", 5);
ratingTable.addRating("Love at last", "Dave", 0);
ratingTable.addRating("Remance forever", "Dave", 0);
ratingTable.addRating("Nonstop car chases", "Dave", 4);

JavaRDD<UserItemRating> input = context.parallelize(ratingTable.getRatings());

ItemCorrelationRecommender recommender = new ItemCorrelationRecommender();

JavaRDD<ItemCorrelation> output = recommender.fitAndTransform(input);

List<ItemCorrelation> predicted = output.collect();
for(ItemCorrelation cell : predicted){
 System.out.println("movie-correlation(" + cell.getItem1() + ", " + cell.getItem2() + "): " + cell.getPearson());
}
```

## Recommend connection via mutual connections

The sample code below shows how to recommend connections between two individual who do not know each other but might have mutual connections:

```java
List<Connection> connections = new ArrayList<>();
connections.add(new Connection("Alice", Arrays.asList("Bob", "Dave"))); // Alice knows Bob and Dave
connections.add(new Connection("Dave", Arrays.asList("Alice", "Carole")));
connections.add(new Connection("Bob", Arrays.asList("James", "Alice", "Jim")));
connections.add(new Connection("Jim", Arrays.asList("Bob", "Smith")));

JavaSparkContext context = SparkContextFactory.createSparkContext("testing-1");
JavaRDD<Connection> connectionJavaRDD = context.parallelize(connections);
ConnectionRecommender recommender = new ConnectionRecommender();
JavaRDD<ConnectionRecommendation> recommendationJavaRDD = recommender.fitAndTransform(connectionJavaRDD);

List<ConnectionRecommendation> recommendations = recommendationJavaRDD.collect();

for(ConnectionRecommendation recommendation : recommendations){
 System.out.println(recommendation.getPerson1() + " can be connected to " + recommendation.getPerson2() + " via " + recommendation.getCommonFriends());
}
```

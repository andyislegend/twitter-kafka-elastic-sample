#### twitter-kafka-elastic-sample
Example simple application that use twitter-hbc to get steam of tweets on topic we point within the app, save them to Kafka, filter through silly Kafka-Stream an then consumed and redirected to Elasticsearch

#how to start

  #prerequisites:
  - Twitter Dev account
  - Kafka 2^ running on port 9092
  - Zookeeper running on port 2181
  - fully configured elastic (https://bonsai.io/ free tier highly recommended)
  
  1) TwitterClient.class
  
  You have to fill this with your Twitter Dev Credentials
  ```java
  private static final String CONSUMER_KEY = "";
  private static final String CONSUMER_SEC = "";
  private static final String ACCESS_TOKEN = "";
  private static final String ACCESS_SECRET = "";
  ```
  2) ElasticClient#bootstrapClient
  
  You have to fill this with Elasticsearch creds (in case of bonsai) or just with elastic location
  ```java
  String username = "";
  String password = "";
  String hostname = "";
  ```
  
 *If everything is set correctly you will be able to run code*
 
 Start *TweeterToKafkaProcessorStarter* for querying twitter
 
 Start *KafkaToElasticProcessorStarter* for producing data from Kafka to Elastic
 
 Start *KafkaStreamFilterStarter* to perform Stream transformation

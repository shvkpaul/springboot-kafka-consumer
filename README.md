# Technologies Used

1. Language : Java 17
2. Framework : Spring Boot 3
3. Kafka
4. Maven
5. H2 Database

# Build/Execute

Import this maven project in your IDE (I am using IntelliJ IDEA).

## H2 console

H2 console : http://localhost:8092/h2-console

* Database query
  SELECT * FROM LIBRARY_EVENT;
  SELECT * FROM BOOK ;

## Kafka 
```
List the topics in a cluster
docker exec --interactive --tty kafka1 kafka-topics --bootstrap-server kafka1:19092 --list
```

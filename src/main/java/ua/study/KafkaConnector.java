package ua.study;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaConnector {

    private SparkSession spark;

    public KafkaConnector(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> readTopic(String bootstrapServers, String topicName) {
        Dataset<Row> rawHotelWeather = spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topicName)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
//                .option("maxOffsetsPerTrigger", 100000)   // KafkaSourceProvider: maxOffsetsPerTrigger option ignored in batch queries
                .load();
        return rawHotelWeather;
    }
}

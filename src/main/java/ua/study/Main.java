package ua.study;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark.*;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import ua.study.tasks.*;

public class Main {
    public static void main(String[] args) {


        // Init
        SparkSession spark = SparkSession
                .builder()
                .appName("ElasticHomeWork")
//                .config("spark.es.nodes","sandbox-hdp.hortonworks.com")
//                .config("spark.es.port","9200")
                .getOrCreate();    // Spark Session

        spark.conf().set("spark.driver.memory", "1g");
        spark.conf().set("spark.sql.shuffle.partitions", "5");                                          // reduce amount of partitions to 5
        spark.conf().set("spark.sql.streaming.schemaInference", true);                                  // turn on the schema inference in stream mode
        Logger.getRootLogger().setLevel(Level.ERROR);                                                   // In order to reduce number of messages in the console


        // Task1.1 Read Expedia data for 2016 year from HDFS as initial state DataFrame
        Task1 task1 = new Task1();
        Dataset<Row> expedia2016 = task1.readHdfsBatch(spark, "hdfs:///user/root/temp/expediaResult/year=2016");

        // Task1.2 Read Expedia data for 2017 year as stream.
        Dataset<Row> expedia2017stream = task1.readHdfsStream(spark, "hdfs:///user/root/temp/expediaResult/year=2017");


        // Task2. Enrich both DataFrames with weather: add average day temperature at checkin (join with hotels+weaher data from Kafka topic)
        // 2.1 Read HotelWeather from Kafka topic "filtered_hw"
        Task2 task2 = new Task2();
        Dataset<Row> hotelWeather = task2.readHotelWeather(new KafkaConnector(spark), "filtered_hw");


        // 2.2 Enrich both DataFrames with weather
        Dataset<Row> expedia2016enriched = task2.enrichExpedia(expedia2016, hotelWeather);
        Dataset<Row> expedia2017enriched = task2.enrichExpedia(expedia2017stream, hotelWeather);


        // Task3. Filter incoming data by having average temperature more than 0 Celsius degrees.
        Task3 task3 = new Task3();
        Dataset<Row> expedia2016enrichedFiltered = task3.filter(expedia2016enriched, "wavg_tmpr_c > 0");
        Dataset<Row> expedia2017enrichedFiltered = task3.filter(expedia2017enriched, "wavg_tmpr_c > 0");

        // Task4. Calculate customer's duration of stay as days between requested check-in and check-out date.
        Task4 task4 = new Task4();
        Dataset<Row> expedia2016duration = task4.addDurationColumn(expedia2016enrichedFiltered);
        Dataset<Row> expedia2017duration = task4.addDurationColumn(expedia2017enrichedFiltered);


        // Task5. Create customer preferences of stay time based on next logic.
        // 5.1 Map each hotel with multi-dimensional state consisting of record counts for each type of stay:
        // "Erroneous data": null, more than month(30 days), less than or equal to 0 - errStay
        // "Short stay": 1 day stay - shortStay
        // "Standart stay": 2-7 days - standartStay
        // "Standart extended stay": 1-2 weeks - extendedStay
        // "Long stay": 2-4 weeks (less than month) - longStay

//        Task5 task5 = new Task5();
//        Dataset<Row> expedia2016preferences = task5.addPreferences(expedia2016duration);
//        Dataset<Row> expedia2017preferences = task5.addPreferences(expedia2017duration);

        // Task 5.2 And resulting type for a hotel (with max count)
//        Dataset<Row> expedia2016hotelType = task5.addHotelType(expedia2016preferences);
//        Dataset<Row> expedia2017hotelType = task5.addHotelType(expedia2017preferences);


        // Task 6. Apply additional variance with filter on children presence in booking (x2 possible states)
//        Task6 task6 = new Task6();
//        Dataset<Row> expedia2016hotelTypeGroupedByChildren = task6.addHotelTypeGroupedByChildren(expedia2016duration);
//        Dataset<Row> expedia2017hotelTypeGroupedByChildren = task6.addHotelTypeGroupedByChildren(expedia2017duration);
//        expedia2016hotelTypeGroupedByChildren.show();

        // Task 5_6. This task includes 2 previous tasks. Here we create the next columns: Hotel type, Preferences, Hotel type by children
        Task5_6 task5_6 = new Task5_6();
        Dataset<Row> expedia2016allColumns = task5_6.expediaAllColumns(expedia2016duration);
        Dataset<Row> expedia2017allColumns = task5_6.expediaAllColumns(expedia2017duration);

        // Task 7. Store final data in HDFS
        Task7 task7 = new Task7();
        task7.storeBatchInHDFS(expedia2016allColumns, "overwrite", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/temp/sparkResult/year=2016");
        task7.storeStreamingInHDFS(expedia2017allColumns,
                "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/temp/sparkCheckPoint",
                "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/temp/sparkResult/year=2017", 120000L);


//        expedia2016allColumns.show();


/*

        // Print to console
        try{
            expedia2017allColumns.filter("hotel_id > 25769803781").writeStream()     // filter("hotel_id > 25769803781").   filter("srch_children_cnt > 2").
                    .outputMode(OutputMode.Complete())
                    .format("console")
                    .option("truncate", false)
                    .start()
                    .awaitTermination(60_000);
        } catch (StreamingQueryException e) {
            System.out.println("There was Exception in method awaitTermination during printing data to console\n" + e);
        }
//*/

    }
}

















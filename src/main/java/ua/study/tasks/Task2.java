package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import ua.study.KafkaConnector;
import ua.study.entity.HotelWeather;

public class Task2 {

    public Dataset<Row> readHotelWeather(KafkaConnector kafkaConnector, String topicName) {
        Dataset<Row> rawHW = kafkaConnector.readTopic("sandbox-hdp:6667", topicName);
        return HotelWeather.convertRawHWtoStructuredHW(rawHW);
    }

    public Dataset<Row> enrichExpedia(Dataset<Row> expedia, Dataset<Row> hotelWeather) {
        return expedia.join(hotelWeather,
                expedia.col("hotel_id").equalTo(hotelWeather.col("hid"))
                        .and(expedia.col("srch_ci").equalTo(hotelWeather.col("wwthr_date"))))
                .selectExpr("order_id", "user_id", "hotel_id", "hname", "hcountry", "hcity", "haddress", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt", "wavg_tmpr_c");
    }
}

package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class Task5 {

    public Dataset<Row> addPreferences(Dataset<Row> expedia) {
        Dataset<Row> result = expedia
                .withColumn("errStay", when(col("duration").isNull(), 1).when(col("duration").leq(0), 1).when(col("duration").gt(30), 1).otherwise(0))
                .withColumn("shortStay", when(col("duration").equalTo(1), 1).otherwise(0))
                .withColumn("standartStay", when(col("duration").between(2, 6), 1).otherwise(0))
                .withColumn("extendedStay", when(col("duration").between(7, 13), 1).otherwise(0))
                .withColumn("longStay", when(col("duration").between(14, 30), 1).otherwise(0))
                .groupBy("hotel_id", "hname", "hcountry", "hcity", "haddress")
                .agg(sum("errStay").as("errStay"), sum("shortStay").as("shortStay"),
                        sum("standartStay").as("standartStay"), sum("extendedStay").as("extendedStay"), sum("longStay").as("longStay"));
        return result;
    }

    public Dataset<Row> addHotelType(Dataset<Row> expedia) {
        Dataset<Row> result = expedia.selectExpr("*", "greatest(errStay, shortStay, standartStay, extendedStay, longStay) AS max")
        .select(col("*"), expr(
                "CASE WHEN errStay = max THEN 'errStay' " +
                        "WHEN shortStay = max THEN 'shortStay' " +
                        "WHEN standartStay = max THEN 'standartStay' " +
                        "WHEN extendedStay = max THEN 'extendedStay' " +
                        "WHEN longStay = max THEN 'longStay' END"
        ).alias("hotelType")).drop("max");
        return result;
    }
}















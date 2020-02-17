package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.expr;

public class Task5_6 {
    public Dataset<Row> expediaAllColumns(Dataset<Row> expediaWithDurationColumn){

        Dataset<Row> result = expediaWithDurationColumn
                .withColumn("errStay", when(col("duration").isNull(), 1).when(col("duration").leq(0), 1).when(col("duration").gt(30), 1).otherwise(0))
                .withColumn("shortStay", when(col("duration").equalTo(1), 1).otherwise(0))
                .withColumn("standartStay", when(col("duration").between(2, 6), 1).otherwise(0))
                .withColumn("extendedStay", when(col("duration").between(7, 13), 1).otherwise(0))
                .withColumn("longStay", when(col("duration").between(14, 30), 1).otherwise(0))
                .withColumn("errStayC", when(col("duration").isNull(), 1).when(col("duration").leq(0), 1).when(col("duration").gt(30), 1).when(col("srch_children_cnt").gt(0), 1).otherwise(0))
                .withColumn("errStayWC", when(col("duration").isNull(), 1).when(col("duration").leq(0), 1).when(col("duration").gt(30), 1).when(col("srch_children_cnt").equalTo(0), 1).otherwise(0))
                .withColumn("shortStayC", when(col("duration").equalTo(1), 1).when(col("srch_children_cnt").gt(0), 1).otherwise(0))
                .withColumn("shortStayWC", when(col("duration").equalTo(1), 1).when(col("srch_children_cnt").equalTo(0), 1).otherwise(0))
                .withColumn("standartStayC", when(col("duration").between(2, 6), 1).when(col("srch_children_cnt").gt(0), 1).otherwise(0))
                .withColumn("standartStayWC", when(col("duration").between(2, 6), 1).when(col("srch_children_cnt").equalTo(0), 1).otherwise(0))
                .withColumn("extendedStayC", when(col("duration").between(7, 13), 1).when(col("srch_children_cnt").gt(0), 1).otherwise(0))
                .withColumn("extendedStayWC", when(col("duration").between(7, 13), 1).when(col("srch_children_cnt").equalTo(0), 1).otherwise(0))
                .withColumn("longStayC", when(col("duration").between(14, 30), 1).when(col("srch_children_cnt").gt(0), 1).otherwise(0))
                .withColumn("longStayWC", when(col("duration").between(14, 30), 1).when(col("srch_children_cnt").equalTo(0), 1).otherwise(0))
                .withColumn("processingTime", current_timestamp())
                .withWatermark("processingTime", "0 seconds")
                .groupBy(
                        col("hotel_id"),
                        col("hname"),
                        col("hcountry"),
                        col("hcity"),
                        col("haddress"),
                        window(col("processingTime"),"1 seconds"))
                .agg(
                        sum("errStay").as("errStay"),
                        sum("shortStay").as("shortStay"),
                        sum("standartStay").as("standartStay"),
                        sum("extendedStay").as("extendedStay"),
                        sum("longStay").as("longStay"),
                        sum("errStayC").as("errStayC"),
                        sum("errStayWC").as("errStayWC"),
                        sum("shortStayC").as("shortStayC"),
                        sum("shortStayWC").as("shortStayWC"),
                        sum("standartStayC").as("standartStayC"),
                        sum("standartStayWC").as("standartStayWC"),
                        sum("extendedStayC").as("extendedStayC"),
                        sum("extendedStayWC").as("extendedStayWC"),
                        sum("longStayC").as("longStayC"),
                        sum("longStayWC").as("longStayWC")
                )
                .withColumn("max", expr("greatest(errStay, shortStay, standartStay, extendedStay, longStay)"))
                .withColumn("maxKids", expr("greatest(errStayC, shortStayC, standartStayC, extendedStayC, longStayC)"))
                .withColumn("maxWithoutKids", expr("greatest(errStayWC, shortStayWC, standartStayWC, extendedStayWC, longStayWC)"))
                .withColumn("hotelType", expr(
                        "CASE WHEN errStayC = maxKids THEN 'errStay' " +
                                "WHEN shortStayC = maxKids THEN 'shortStay' " +
                                "WHEN standartStayC = maxKids THEN 'standartStay' " +
                                "WHEN extendedStayC = maxKids THEN 'extendedStay' " +
                                "WHEN longStayC = maxKids THEN 'longStay' END"
                ))
                .withColumn("hotelTypeKids", expr(
                        "CASE WHEN errStayC = maxKids THEN 'errStay' " +
                                "WHEN shortStayC = maxKids THEN 'shortStay' " +
                                "WHEN standartStayC = maxKids THEN 'standartStay' " +
                                "WHEN extendedStayC = maxKids THEN 'extendedStay' " +
                                "WHEN longStayC = maxKids THEN 'longStay' END"
                ))
                .withColumn("hotelTypeWithoutKids", expr(
                        "CASE WHEN errStayWC = maxWithoutKids THEN 'errStay' " +
                                "WHEN shortStayWC = maxWithoutKids THEN 'shortStay' " +
                                "WHEN standartStayWC = maxWithoutKids THEN 'standartStay' " +
                                "WHEN extendedStayWC = maxWithoutKids THEN 'extendedStay' " +
                                "WHEN longStayWC = maxWithoutKids THEN 'longStay' END"
                ))
                .drop("max", "maxKids", "maxWithoutKids", "errStayC", "errStayWC", "shortStayC", "shortStayWC", "standartStayC",
                        "standartStayWC", "extendedStayC", "extendedStayWC", "longStayC", "longStayWC", "window");
        return result;
    }
}





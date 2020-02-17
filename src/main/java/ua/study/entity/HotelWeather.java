package ua.study.entity;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class HotelWeather {

    // hid BIGINT, hname STRING,  hcountry STRING, hcity STRING, haddress STRING, wavg_tmpr_c DOUBLE, wwthr_date STRING, prec INT
    public static StructType fullSchema = new StructType()
            .add("hid", DataTypes.LongType)
            .add("hname", DataTypes.StringType)
            .add("hcountry", DataTypes.StringType)
            .add("hcity", DataTypes.StringType)
            .add("haddress", DataTypes.StringType)
            .add("wavg_tmpr_c", DataTypes.DoubleType)
            .add("wwthr_date", DataTypes.StringType)
            .add("prec", DataTypes.IntegerType);

    /*
     *   This function convert raw HotelWeather data that read from HDFS to a DataFrame
     * */
    public static Dataset<Row> convertRawHWtoStructuredHW(Dataset<Row> rawHotelWeather){
        Dataset<Row> hotelWeatherDf = rawHotelWeather
                .select(col("value").cast(DataTypes.StringType))
                .select(from_json(col("value"), HotelWeather.fullSchema).as("hw"))
                .selectExpr("hw.hid AS hid" , "hw.hname AS hname", "hw.hcountry AS hcountry", "hw.hcity AS hcity", "hw.haddress AS haddress", "hw.wavg_tmpr_c AS wavg_tmpr_c", "hw.wwthr_date AS wwthr_date");
        return hotelWeatherDf;
    }
}

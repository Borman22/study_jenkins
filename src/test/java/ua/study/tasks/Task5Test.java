package ua.study.tasks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class Task5Test {

    private SparkSession spark;
    private Task5 task5 = new Task5();


    private static StructType task51InSchema = new StructType()
            .add("hotel_id", DataTypes.LongType)
            .add("hname", DataTypes.StringType)
            .add("hcountry", DataTypes.StringType)
            .add("hcity", DataTypes.StringType)
            .add("haddress", DataTypes.StringType)
            .add("duration", DataTypes.IntegerType);

    private static StructType task51OutSchema = new StructType()
            .add("hotel_id", DataTypes.LongType)
            .add("hname", DataTypes.StringType)
            .add("hcountry", DataTypes.StringType)
            .add("hcity", DataTypes.StringType)
            .add("haddress", DataTypes.StringType)
            .add("errStay", DataTypes.IntegerType)
            .add("shortStay", DataTypes.IntegerType)
            .add("standartStay", DataTypes.IntegerType)
            .add("extendedStay", DataTypes.IntegerType)
            .add("longStay", DataTypes.IntegerType);

    private static StructType task52InSchema = task51OutSchema;

    private static StructType task52OutSchema = new StructType()
            .add("hotel_id", DataTypes.LongType)
            .add("hname", DataTypes.StringType)
            .add("hcountry", DataTypes.StringType)
            .add("hcity", DataTypes.StringType)
            .add("haddress", DataTypes.StringType)
            .add("errStay", DataTypes.IntegerType)
            .add("shortStay", DataTypes.IntegerType)
            .add("standartStay", DataTypes.IntegerType)
            .add("extendedStay", DataTypes.IntegerType)
            .add("longStay", DataTypes.IntegerType)
            .add("hotelType", DataTypes.StringType);


    @Before
    public void setUp() {
        spark = SparkSession
                .builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", "5");
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void addPreferencesTest() {

        List<Row> listIn = new ArrayList<>();
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", -10));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 0));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", null));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 1));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 5));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 6));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 7));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 10));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 13));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 14));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 16));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 18));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 29));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 30));
        listIn.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 100));
        Dataset<Row> expediaIn = spark.createDataFrame(listIn, task51InSchema);

        List<Row> listOut = new ArrayList<>();
        listOut.add(RowFactory.create(1L, "n1", "ci1", "co1", "a1", 4, 1, 2, 3, 5));
        Dataset<Row> expediaOut = spark.createDataFrame(listOut, task51OutSchema);

        Dataset<Row> returnedValue = task5.addPreferences(expediaIn);
        Assert.assertEquals(expediaOut.collectAsList(), returnedValue.collectAsList());
    }

    @Test
    public void addHotelTypeTest() {
        Task5 task5 = new Task5();

        List<Row> listIn = new ArrayList<>();
        listIn.add(RowFactory.create(1L, "n1", "co1", "ci1", "a1", 5, 5, 5, 5, 10));
        listIn.add(RowFactory.create(2L, "n1", "co1", "ci1", "a1", 5, 5, 5, 10, 10));
        listIn.add(RowFactory.create(3L, "n1", "co1", "ci1", "a1", 5, 14, 15, 10, 11));
        listIn.add(RowFactory.create(4L, "n1", "co1", "ci1", "a1", 5, 15, 15, 10, 11));
        listIn.add(RowFactory.create(5L, "n1", "co1", "ci1", "a1", 15, 15, 15, 10, 11));
        Dataset<Row> expediaIn = spark.createDataFrame(listIn, task52InSchema);

        List<Row> listOut = new ArrayList<>();
        listOut.add(RowFactory.create(1L, "n1", "co1", "ci1", "a1", 5, 5, 5, 5, 10, "longStay"));
        listOut.add(RowFactory.create(2L, "n1", "co1", "ci1", "a1", 5, 5, 5, 10, 10, "extendedStay"));
        listOut.add(RowFactory.create(3L, "n1", "co1", "ci1", "a1", 5, 14, 15, 10, 11, "standartStay"));
        listOut.add(RowFactory.create(4L, "n1", "co1", "ci1", "a1", 5, 15, 15, 10, 11, "shortStay"));
        listOut.add(RowFactory.create(5L, "n1", "co1", "ci1", "a1", 15, 15, 15, 10, 11, "errStay"));
        Dataset<Row> expediaOut = spark.createDataFrame(listOut, task52OutSchema);

        Dataset<Row> returnedValue = task5.addHotelType(expediaIn);
        Assert.assertEquals(expediaOut.collectAsList(), returnedValue.collectAsList());
    }
}

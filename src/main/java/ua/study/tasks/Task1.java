package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Task1 {

    public Dataset<Row> readHdfsBatch(SparkSession spark, String sourceDir) {
        return spark
                .read()
                .parquet(sourceDir);
    }

    public Dataset<Row> readHdfsStream(SparkSession spark, String sourceDir) {
        return spark
                .readStream()
                .option("maxFilesPerTrigger", 1)
                .parquet(sourceDir);
    }
}



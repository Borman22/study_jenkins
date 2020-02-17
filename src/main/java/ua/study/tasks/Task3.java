package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Task3 {

    public Dataset<Row> filter(Dataset<Row> expedia, String condition) {
        return expedia.filter(condition);
    }
}

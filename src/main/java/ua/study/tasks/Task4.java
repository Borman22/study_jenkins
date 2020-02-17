package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Task4 {

    public Dataset<Row> addDurationColumn(Dataset<Row> expedia) {
        return expedia.selectExpr("*", "datediff(srch_co, srch_ci) AS duration");
    }
}



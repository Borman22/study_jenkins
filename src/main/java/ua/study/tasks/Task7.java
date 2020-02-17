package ua.study.tasks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Task7 {

    public void storeBatchInHDFS(Dataset<Row> expediaBatch, String mode, String path) {
        expediaBatch.write().mode(mode).parquet(path);
    }

    public void storeStreamingInHDFS(Dataset<Row> expedia, String checkpointLocation, String path, long timeout) {
        try {
            expedia
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .queryName("HomeWork")
                    .format("parquet")
                    .option("checkpointLocation", checkpointLocation)
                    .option("path", path)
                    .start()
                    .awaitTermination(timeout);
        } catch (StreamingQueryException e) {
            System.out.println("There was Exception in method awaitTermination\n" + e);
        }
    }


}



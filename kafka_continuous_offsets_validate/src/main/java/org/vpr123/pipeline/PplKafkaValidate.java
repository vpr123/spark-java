package org.vpr123.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.vpr123.generic.Utils;
import java.io.IOException;

public class PplKafkaValidate {

    public static Dataset<Row> run(SparkSession spark, Dataset<Row> kafka_messages_df, String start_date, Integer start_offset) throws IOException {

        Utils.createView(kafka_messages_df, "kafka_messages");

        Utils.runSQL(spark, "sql/01_events_dedup.sql",
                new String[][] {
                        {"start_date", start_date},
                        {"start_offset", String.valueOf(start_offset)}
                },
                "t_stg_events");

        Utils.runSQL(spark, "sql/02_event_continuous_ranges.sql", "t_stg_event_continuous_ranges");

        return Utils.runSQL(spark, "sql/03_missing_offsets.sql");

    }

}
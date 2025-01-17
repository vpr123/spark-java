package org.vpr123.pipeline;

//import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.vpr123.generic.Helpers;

import static org.vpr123.generic.Helpers.say;
import static org.junit.Assert.assertTrue;

//@Log4j2
public class PplKafkaValidateTest {

    @Test
    public void Test() throws Throwable {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQL")
                .master("local[*]")
                .appName("Java Spark SQL example")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> res = PplKafkaValidate.run(
                spark,
                spark.read().option("header", "true").csv("./src/test/resources/data/kafka_messages.csv"),
                "2024-12-28",
                10);

        Helpers.runQuery(spark,"select * from kafka_messages", 20);
        Helpers.runQuery(spark,"select * from t_stg_events", 20);
        Helpers.runQuery(spark,"select * from t_stg_event_continuous_ranges");

        say("RESULT");
        res.show();

        int expected_row_count = 3;
        say("res.count() = " + res.count());
        assertTrue(res.count() == expected_row_count);
        say("Final dataset row count = " + expected_row_count + " validation passed");

        spark.stop();

    }

}

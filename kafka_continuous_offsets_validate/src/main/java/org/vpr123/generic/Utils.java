package org.vpr123.generic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Utils {

    public static List<Row> getResourceData(String file_path) throws IOException {

        InputStream is = Utils.class.getClassLoader().getResourceAsStream(file_path);
        if (is == null) {
            throw new FileNotFoundException("file not found! " + file_path);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line = null;
        List<Row> result = new ArrayList<Row>();
        while ((line = reader.readLine()) != null) {
        }
        result.add(RowFactory.create(line.split(",")));
        return result;
    }

    public static void createView(Dataset<Row> df, String view_name) {
        System.out.println("\n-- " + view_name + "\n");
        df.createOrReplaceTempView(view_name);
        df.printSchema();
        //df.show();
    }

    public static String getSQL(String file_path) throws IOException {
        InputStream is = Utils.class.getClassLoader().getResourceAsStream(file_path);
        if (is == null) {
            throw new FileNotFoundException("file not found!" + file_path);
        }
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines().collect(Collectors.joining("\n"));
    }

    public static Dataset<Row> runSQL (SparkSession spark, String file_path) throws IOException {
        String s = Utils.getSQL (file_path);
        System.out.println("\n" + s);
        Dataset<Row> res = spark.sql(s);
        res.printSchema();
        //res.show();
        return res;
    }

    public static void runSQL (SparkSession spark, String file_path, String view_name) throws IOException {
        String s = getSQL (file_path);
        System.out.println("\n-- " + view_name + "\n" + s);
        Dataset<Row> res = spark.sql(s);
        res.createOrReplaceTempView(view_name);
        res.printSchema();
        //res.show();
    }

    public static void runSQL(SparkSession spark, String file_path, String params[][], String view_name) throws IOException {

        String s = getSQL(file_path);

        for (int i = 0; i < params.length; i++) {
            s = s.replace("$$" + params[i][0] + "$$", params[i][1]);
        }

        System.out.println("\n-- " + view_name + "\n" + s);
        Dataset<Row> res = spark.sql(s);
        res.createOrReplaceTempView(view_name);
        res.printSchema();
        //res.show();
    }

}


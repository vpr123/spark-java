package org.vpr123.generic;

//import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

//@Log4j2
public class Helpers {

    public static void runQuery(SparkSession spark, String query) throws IOException {
        System.out.println("\n===> " + query + "\n");
        Dataset<Row> res = spark.sql(query);
        res.show(100, false);
    }

    public static void runQuery(SparkSession spark, String query, int num_rows) throws IOException {
        System.out.println("\n===> " + query + "\n");
        Dataset<Row> res = spark.sql(query);
        res.show(num_rows, false);
    }

    public static String getSchemaString(String schema_path) throws Throwable {
        JSONParser jsonParser = new JSONParser();
        FileReader reader = new FileReader(schema_path);
        Object obj = jsonParser.parse(reader);
        JSONArray columnList = (JSONArray) obj;

        List<String> lst = new ArrayList<String>();

        Iterator<JSONObject> iterator = columnList.iterator();
        while (iterator.hasNext()) {
            lst.add(iterator.next().get("fieldName").toString());
        }

        return lst.stream().collect(Collectors.joining(","));
    }

    public static String getSchemaWithTypesString(String schema_path) throws Throwable {
        JSONParser jsonParser = new JSONParser();
        FileReader reader = new FileReader(schema_path);
        Object obj = jsonParser.parse(reader);
        JSONArray columnList = (JSONArray) obj;

        List<String> lst = new ArrayList<String>();

        Iterator<JSONObject> iterator = columnList.iterator();
        while (iterator.hasNext()) {
            JSONObject item = iterator.next();
            String dataTypeJson = item.get("dataType").toString();
            String dataTypeSchema;
            if (dataTypeJson.equals("BIGINT")) {
                dataTypeSchema = "LONG";
            } else {
                dataTypeSchema = dataTypeJson;
            }

            lst.add(item.get("fieldName").toString() + ":" + dataTypeSchema);
        }

        return lst.stream().collect(Collectors.joining(","));
    }

    public static String getDatasetString(String dataset_path) throws Throwable {
        JSONParser jsonParser = new JSONParser();
        FileReader reader = new FileReader(dataset_path);
        Object obj = jsonParser.parse(reader);
        JSONObject jo = (JSONObject) obj;
        JSONArray columnList = (JSONArray) jo.get("fields");
        List<String> lst = new ArrayList<String>();
        Iterator<JSONObject> iterator = columnList.iterator();
        while (iterator.hasNext()) {
            lst.add(iterator.next().get("fieldName").toString());
        }
        return lst.stream().collect(Collectors.joining(","));
    }

    public static void say(String txt) {
        //log.info("===> " + txt);
        System.out.println("===> " + txt);
    }

    public static String getSchemaFileName(String folder_path, String dataset_name) throws Throwable {
        File folder = new File(folder_path);
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().startsWith(dataset_name + "#")) {
                return listOfFiles[i].getName();
            }
        }
        return "?";
    }

}
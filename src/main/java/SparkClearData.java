import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SparkClearData {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        try (Stream<Path> walk = Files.walk(Paths.get(args[0]))) {
            int ok = 0;
            int notOK = 0;

            FileWriter fileToLoad = new FileWriter(args[1]);
            List<String> result = walk.filter(Files::isRegularFile)
                    .map(x -> x.toString()).collect(Collectors.toList());
            for (String path : result
            ) {
                JavaRDD<Row> file = spark.read().option("header", "true").option("delimiter", ",").csv(path).javaRDD();
                String schema = file.first().schema().toString();
                if (schema.contains("TMAX") && schema.contains("TAVG") && schema.contains("TMIN")) {
                    fileToLoad.write(path+System.lineSeparator());
                    ok++;
                } else {
                    notOK++;
                }

            }

            fileToLoad.close();
            System.out.println("Ok Count: "+ok);
            System.out.println("Not Ok Count: "+notOK);


        } catch (IOException e) {
            e.printStackTrace();
        }




        spark.stop();
    }
}
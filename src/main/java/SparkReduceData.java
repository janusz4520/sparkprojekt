import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.execution.columnar.DOUBLE;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.interfaces.DSAPublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SparkReduceData {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();

        try {
            List<String> result;
            try(Stream<String> lines = Files.lines(Paths.get(args[0]))){
            result= lines.collect(Collectors.toList());}
            FileWriter outputData = new FileWriter(args[1]);
            outputData.write("NAME,LONGITUDE,LATITUDE,TAMAX,TAMAXDATE,TAMIN,TAMINDATE\n");
            for (String path : result
            ) {
                try {
                    JavaRDD<Row> file = spark.read().option("header", "true").option("delimiter", ",").option("nullValue", null).csv(path).javaRDD();

                    JavaRDD<Row> reduced = file.map(clearData());
                    Row maxTAVG = reduced.reduce(getMAX());
                    Row minTAVG = reduced.reduce(getMIN());

                    System.out.println(path);
                    System.out.println(maxTAVG.size());
                    System.out.println("DATE: "+ maxTAVG.getInt(1));
                    System.out.println("MAX: " + maxTAVG.getDouble(6));
                    System.out.println("DATE: "+minTAVG.getInt(1));
                    System.out.println("TMIN: " + minTAVG.getDouble(6));


                    outputData.write(ToString(maxTAVG, minTAVG));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            outputData.close();



        } catch (Exception e) {
            e.printStackTrace();
        }


        spark.stop();
    }

    private static String ToString(Row maxTAVG, Row minTAVG) {
        StringBuilder sb = new StringBuilder();
        sb.append(maxTAVG.getString(0));
        sb.append(",");
        sb.append(maxTAVG.getDouble(2));
        sb.append(",");
        sb.append(maxTAVG.getDouble(3));
        sb.append(",");
        sb.append(maxTAVG.getDouble(6));
        sb.append(",");
        sb.append(maxTAVG.getInt(1));
        sb.append(",");
        sb.append(minTAVG.getDouble(6));
        sb.append(",");
        sb.append(minTAVG.getInt(1));
        sb.append("\n");
        return sb.toString();
    }

    private static Function2<Row, Row, Row> getMIN() {
        return (a,b)->{
            Double TAVGa=null;
            Double TAVGb=null;
            try{TAVGa = a.getDouble(6);}catch (Exception e){}
            try{TAVGb = b.getDouble(6);}catch (Exception e){}

            if(TAVGb==null)
                return a;
            if (TAVGa==null)
                return b;
            if (TAVGa<=TAVGb)
                return a;
            else
                return b;
        };
    }

    private static Function2<Row, Row, Row> getMAX() {
        return (a,b) ->{
            Double TAVGa=null;
            Double TAVGb=null;
            try{TAVGa = a.getDouble(6);}catch (Exception e){}
            try{TAVGb = b.getDouble(6);}catch (Exception e){}
            if (TAVGa==null)
                return b;
            if(TAVGb==null)
                return a;
            if (TAVGa>=TAVGb)
                return a;
            else
                return b;
        };
    }

    private static Function<Row, Row> clearData() {
        return s -> {
            Integer DATE=null;
            Double TMAX=null;
            Double TMIN=null;
            Double TAVG=null;
            Double LONG=null;
            Double LAT=null;
            String NAME = null;
            try{DATE = Integer.parseInt(s.getString(s.fieldIndex("DATE")));}catch (Exception e){}

            try{TMAX = Double.parseDouble(s.getString(s.fieldIndex("TMAX")));}catch (Exception e){}
            try{TMIN = Double.parseDouble(s.getString(s.fieldIndex("TMIN")));}catch (Exception e){}
            try{TAVG = Double.parseDouble(s.getString(s.fieldIndex("TAVG")));}catch (Exception e){}
            try{LONG = Double.parseDouble(s.getString(s.fieldIndex("LONGITUDE")));}catch (Exception e){}
            try{LAT = Double.parseDouble(s.getString(s.fieldIndex("LATITUDE")));}catch (Exception e){}
            try{NAME = s.getString(s.fieldIndex("NAME"));}catch (Exception e){}

            return RowFactory.create(NAME,DATE,LONG,LAT,TMAX,TMIN,TAVG);
        };
    }
}
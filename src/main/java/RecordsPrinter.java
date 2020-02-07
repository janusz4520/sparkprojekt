import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class RecordsPrinter {

    public static void main(String[] args) throws IOException {
        Reader input = new FileReader("E:\\projekty\\programowanie\\sparkprojekt\\src\\main\\resources\\outputYearly.csv");

        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(input);

        Integer dataSize = 0;
        Integer higherTemperatue = 0;

        for (CSVRecord record : records){
            Double maxAverageTemperatureYear = parseInt(record.get(CsvLabel.TAMAXDATE));
            Double minAverageTemperatureYear = parseInt(record.get(CsvLabel.TAMINDATE));

            if(maxAverageTemperatureYear > minAverageTemperatureYear)
                higherTemperatue++;
            dataSize++;
        }

        System.out.println("Total: "+ dataSize);
        System.out.println("Has grown: "+ higherTemperatue);
        System.out.println("Temperature higher in "+ higherTemperatue * 100 /dataSize + " percent of cases");
    }

    private static Double parseInt(String s){
        return Double.valueOf(s);
    }

    public enum CsvLabel{
        NAME,COUNTRY,LONGITUDE,LATITUDE,TAMAX,TAMAXDATE,TAMIN,TAMINDATE;
    }

}
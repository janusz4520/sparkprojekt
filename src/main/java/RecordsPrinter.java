import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class RecordsPrinter {

    public static void main(String[] args) throws IOException {
        Reader input = new FileReader("E:\\projekty\\programowanie\\sparkprojekt\\src\\main\\resources\\outputYearly.csv");

        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .parse(input);

        Integer dataSize = 0;
        Integer higherTemperatue = 0;

        List<Double> temperatureDifference = new ArrayList<>();

        for (CSVRecord record : records){
            Double maxAverageTemperatureYear = parseDouble(record.get(CsvLabel.TAMAXDATE));
            Double minAverageTemperatureYear = parseDouble(record.get(CsvLabel.TAMINDATE));
            Double minAverageTemperature = parseDouble(record.get(CsvLabel.TAMIN));
            Double maxAverageTemperature = parseDouble(record.get(CsvLabel.TAMAX));

            if(maxAverageTemperatureYear > minAverageTemperatureYear){
                higherTemperatue++;

                temperatureDifference.add( minAverageTemperature - maxAverageTemperature );
            }
            temperatureDifference.add( maxAverageTemperature - minAverageTemperature );


            dataSize++;
        }


        System.out.println("Avg difference: " + calculateAvg(temperatureDifference));
        System.out.println("Total: "+ dataSize);
        System.out.println("Has grown: "+ higherTemperatue);
        System.out.println("Temperature higher in "+ calculatePercentage(dataSize, higherTemperatue) + " percent of cases");
    }

    private static Double calculateAvg(List<Double> list){
        return list.stream()
                .mapToDouble(a -> a)
                .average().getAsDouble();
    }

    private static int calculatePercentage(Integer totalSize, Integer value) {
        return value * 100 /totalSize;
    }

    private static Double parseDouble(String s){
        return Double.valueOf(s);
    }

    public enum CsvLabel{
        NAME,COUNTRY,LONGITUDE,LATITUDE,TAMAX,TAMAXDATE,TAMIN,TAMINDATE;
    }

}
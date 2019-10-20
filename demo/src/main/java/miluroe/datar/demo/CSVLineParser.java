package miluroe.datar.demo;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.function.Function;

import java.io.IOException;
import java.io.StringReader;

public class CSVLineParser implements Function<String,String[]> {


    @Override
    public String[] call(String line) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(line));
        String[] data = null;
        try {
            data = reader.readNext();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return data;
    }
}

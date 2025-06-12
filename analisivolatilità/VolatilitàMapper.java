package stockanalisis.volatilita;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VolatilitaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // header: ticker,date,close,volume,sector
        String line = value.toString();
        if (line.startsWith("ticker,")) return;
        String[] parts = line.split(",", -1);
        if (parts.length >= 5) {
            String sector = parts[4];
            String date = parts[1];
            String year = date.substring(0, 4);
            double close = Double.parseDouble(parts[2]);
            context.write(new Text(sector + "#" + year), new DoubleWritable(close));
        }
    }
}

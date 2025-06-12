package stockanalisis.analisisingoloticker;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TickerAnalisisMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // header: ticker,date,close,volume,sector
        String line = value.toString();
        if (line.startsWith("ticker,")) return;
        String[] parts = line.split(",", -1);
        if (parts.length >= 6) {
            String ticker = parts[0];
            String name = parts[1];
            String date = parts[2];
            String close = parts[4];
            String year = date.substring(0, 4);
            // chiave: ticker#anno, valore: date,close
            context.write(new Text(ticker + "#" + year), new Text(date + "," + close));
        }
    }
}

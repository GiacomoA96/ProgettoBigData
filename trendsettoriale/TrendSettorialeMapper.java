package stockanalisis.trendsettoriale;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrendSettorialeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // header: ticker,date,close,volume,sector
        String line = value.toString();
        if (line.startsWith("ticker,")) return;
        String[] parts = line.split(",", -1);
        if (parts.length >= 5) {
            String ticker = parts[0];
            String date = parts[1];
            String close = parts[2];
            String volume = parts[3];
            String sector = parts[4];
            String year = date.substring(0, 4);
            if(sector != null && !sector.isEmpty()){
                context.write(new Text(sector + "#" + year), new Text(ticker + "," + date + "," + close + "," + volume));
            }    
        }
    }
}
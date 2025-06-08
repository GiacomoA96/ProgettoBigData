package TrendSettoriale;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SectorYearMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> tickerToSector = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        if (files != null && files.length > 0) {
            BufferedReader reader = new BufferedReader(new FileReader(new File(files[0].getPath())));
            String line;
            boolean firstLine = true;
            while ((line = reader.readLine()) != null) {
                if (firstLine) { firstLine = false; continue; }
                String[] parts = line.split(",", -1);
                if (parts.length >= 5) {
                    String ticker = parts[0];
                    String sector = parts[3];
                    tickerToSector.put(ticker, sector);
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // header: ticker,open,close,low,high,volume,date
        String line = value.toString();
        if (line.startsWith("ticker,")) return;
        String[] parts = line.split(",", -1);
        if (parts.length >= 8) {
            String ticker = parts[1];
            String closeStr = parts[5];
            String volumeStr = parts[6];
            String dateStr = parts[7];
            String sector = tickerToSector.get(ticker);
            if (sector != null && !sector.isEmpty()) {
                try {
                    double close = Double.parseDouble(closeStr);
                    long volume = Long.parseLong(volumeStr);
                    String year = dateStr.substring(0, 4);
                    // chiave: settore#anno
                    context.write(new Text(sector + "#" + year), new Text(ticker + "," + dateStr + "," + close + "," + volume));
                } catch (Exception e) {
                    // ignora righe malformate
                }
            }
        }
    }
}
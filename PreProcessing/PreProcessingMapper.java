package PreProcessing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PreprocessingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Map<String, String[]> tickerInfo = new HashMap<>();

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
                    // Salva tutte le info aziendali tranne il ticker
                    tickerInfo.put(ticker, new String[]{parts[1], parts[2], parts[3], parts[4]});
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
            String[] info = tickerInfo.get(ticker);
            if (info != null) {
                // Unisci le informazioni: prezzi storici + info aziendali
                String joined = String.join(",", parts) + "," + String.join(",", info);
                context.write(NullWritable.get(), new Text(joined));
            }
        }
    }
}

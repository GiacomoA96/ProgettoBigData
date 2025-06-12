package stockanalisis.preprocessing;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PreprocessingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    // Mappa: ticker -> [nome, settore]
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
                    String name = parts[2];
                    String sector = parts[3];
                    tickerInfo.put(ticker, new String[]{name, sector});
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Andiamo a leggere gli input del file CSV che avrà come header:
        // ticker,name,open,close,low,high,volume,date
        String line = value.toString();
        if (line.startsWith("ticker,")) return;
        String[] parts = line.split(",", -1);
        if (parts.length >= 8) {
            String ticker = parts[0];
            String close = parts[2];
            String volume = parts[6];
            String date = parts[7];
            String[] info = tickerInfo.get(ticker);
            if (info != null) {
                String name = info[0];
                String sector = info[1];
                // in questo modo possiamo creare un output che contiene il ticker, il nome, la data, il prezzo di chiusura, il volume e il settore
                if(!ticker.isEmpty() && !name.isEmpty() && !date.isEmpty() && !close.isEmpty() && !volume.isEmpty() && !sector.isEmpty()) {
                    // Viene creata una stringa di output con i campi richiesti
                    String output = String.join(",", ticker, name, date, close, volume, sector);
                    context.write(new Text(ticker), new Text(output)); // Utilizziamo ticker come chiave
                }
            }
        }
    }
}

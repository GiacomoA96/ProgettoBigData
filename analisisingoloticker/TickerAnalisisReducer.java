package stockanalisis.analisisingoloticker;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TickerAnalisisReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String minDate = null, maxDate = null;
        double closeStart = 0, closeEnd = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length != 3) continue;
            String name = parts[0];
            String date = parts[1];
            double close = Double.parseDouble(parts[2]);

            if (minDate == null || date.compareTo(minDate) < 0) {
                minDate = date;
                closeStart = close;
            }
            if (maxDate == null || date.compareTo(maxDate) > 0) {
                maxDate = date;
                closeEnd = close;
            }
        }

        if (minDate != null && maxDate != null && closeStart != 0) {
            double percChange = (closeEnd - closeStart) / closeStart * 100;
            context.write(key, new Text("Nome=" + name +",VariazionePercentuale=" + String.format("%.2f", percChange) + "%, PrezzoInizio=" + closeStart + ", PrezzoFine=" + closeEnd));
        }
    }
}
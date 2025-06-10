package analisisingoloticker;

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
            if (parts.length != 2) continue;
            String date = parts[0];
            double close = Double.parseDouble(parts[1]);

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
            context.write(key, new Text("VariazionePercentuale=" + String.format("%.2f", percChange) + "%, PrezzoInizio=" + closeStart + ", PrezzoFine=" + closeEnd));
        }
    }
}
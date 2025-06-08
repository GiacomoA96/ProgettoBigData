package TrendSettoriale;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SectorYearReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalVolume = 0;
        double sumClose = 0;
        int count = 0;

        // Per trovare il prezzo a inizio e fine anno
        String minDate = null, maxDate = null;
        double closeStart = 0, closeEnd = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length != 4) continue;
            String ticker = parts[0];
            String date = parts[1];
            double close = Double.parseDouble(parts[2]);
            long volume = Long.parseLong(parts[3]);

            totalVolume += volume;
            sumClose += close;
            count++;

            // Trova la data minima e massima
            if (minDate == null || date.compareTo(minDate) < 0) {
                minDate = date;
                closeStart = close;
            }
            if (maxDate == null || date.compareTo(maxDate) > 0) {
                maxDate = date;
                closeEnd = close;
            }
        }

        double percChange = (closeEnd - closeStart) / closeStart * 100;
        double avgClose = sumClose / count;

        context.write(key, new Text(
            "VolumeTotale=" + totalVolume +
            ",PrezzoInizioAnno=" + closeStart +
            ",PrezzoFineAnno=" + closeEnd +
            ",VariazionePercentuale=" + String.format("%.2f", percChange) + "%" +
            ",PrezzoMedioGiornaliero=" + String.format("%.2f", avgClose)
        ));
    }
}
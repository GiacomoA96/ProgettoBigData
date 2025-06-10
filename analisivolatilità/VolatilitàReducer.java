package analisivolatilità;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VolatilitàReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        List<Double> closes = new ArrayList<>();
        double sum = 0;
        int count = 0;

        for (DoubleWritable val : values) {
            double close = val.get();
            closes.add(close);
            sum += close;
            count++;
        }

        double mean = sum / count;
        double sumSq = 0;
        for (double close : closes) {
            sumSq += (close - mean) * (close - mean);
        }
        double stddev = Math.sqrt(sumSq / count);

        context.write(key, new Text("Volatilita(StdDev)=" + String.format("%.4f", stddev) + ", PrezzoMedio=" + String.format("%.2f", mean)));
    }
}

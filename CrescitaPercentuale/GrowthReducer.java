package CrescitaPercentuale;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GrowthReducer extend Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double initialClose1998 = 0.0;
        double finalClose2018 = 0.0;
        boolean foundInitial = false;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length != 2) continue;

            String date = parts[0];
            double closePrice = Double.parseDouble(parts[1]);

            if (date.startsWith("1998") && !foundInitial) {
                initialClose1998 = closePrice;
                foundInitial = true;
            } else if (date.startsWith("2018")) {
                finalClose2018 = closePrice;
            }
        }

        if (foundInitial && finalClose2018 > 0) {
            double growthPercentage = ((finalClose2018 - initialClose1998) / initialClose1998) * 100;
            result.set(String.format("%.2f", growthPercentage));
            context.write(key, result);
        }
    }
    
}

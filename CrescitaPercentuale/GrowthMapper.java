package CrescitaPercentuale;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GrowthMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text ticket = new Text();
    private Text dateClose = new Text();

    @Override
    private void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        try{
            String[] fields = value.toString.split(",");
            if(fields.length < 7)
                return;
            String date = fields[0];
            String symbol = fields[1];
            String close = fields[5];

            if(date.startsWith("1998") || date.startsWith("2018")) {
                ticket.set(symbol);
                dateClose.set(date + "," + close);
                context.write(ticket, dateClose);
            }
        }
        catch (Exception e) {
            // Log the exception or handle it as needed
            System.err.println("Error processing line: " + value.toString() + " - " + e.getMessage());
        }
    }
}
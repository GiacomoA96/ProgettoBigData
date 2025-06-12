package stockanalisis.trendsettoriale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import stockanalisis.trendsettoriale.TrendSettorialeMapper;
import stockanalisis.trendsettoriale.TrendSettorialeReducer;

public class TrendSettoriale extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SectorYearDriver <input_preprocessed> <output>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Trend Settoriale per Anno");
        job.setJarByClass(TrendSettoriale.class);
        job.setMapperClass(TrendSettorialeMapper.class);
        job.setReducerClass(TrendSettorialeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new TrendSettoriale(), args);
        System.exit(exitCode);
    }
}
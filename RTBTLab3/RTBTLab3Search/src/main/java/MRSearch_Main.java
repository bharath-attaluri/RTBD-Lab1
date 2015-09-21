import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MRSearch_Main {

    public static String KEY_WORD = "key";

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        Configuration config = new Configuration(true);

        config.set(KEY_WORD, args[2]);

        Job job = new Job(config, "Searching");
        job.setJarByClass(MRSearch_Map.class);

        job.setMapperClass(MRSearch_Map.class);
        job.setReducerClass(MRSearch_Reduce.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(config);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);

    }
}
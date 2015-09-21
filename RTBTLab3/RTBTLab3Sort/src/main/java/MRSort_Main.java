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

    public class MRSort_Main {

        public  void main(String[] args) throws IOException,
                InterruptedException, ClassNotFoundException {

            Path inputPath = new Path(args[0]);
            Path outputDir = new Path(args[1]);

            Configuration conf = new Configuration(true);


            Job job = new Job(conf, "SortingData");
            job.setJarByClass(MRSort_Map.class);

            job.setMapperClass(MRSort_Map.class);
            job.setReducerClass(MRSort_Reduce.class);
            job.setCombinerClass(MRSort_Reduce.class);
            job.setNumReduceTasks(1);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, outputDir);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileSystem hdfs = FileSystem.get(conf);
            if (hdfs.exists(outputDir))
                hdfs.delete(outputDir, true);

            int code = job.waitForCompletion(true) ? 0 : 1;
            System.exit(code);

        }

    }
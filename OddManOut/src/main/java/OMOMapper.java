/**
 * Created by Mayanka on 03-Sep-15.
 */

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OMOMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private final Text ONE = new Text("unique");
    private IntWritable word = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(" ");
        for (String str : csv) {
            word.set(Integer.parseInt(str));
            context.write(ONE, word);
        }
    }
}

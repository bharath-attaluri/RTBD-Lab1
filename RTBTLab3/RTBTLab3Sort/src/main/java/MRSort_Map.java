import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MRSort_Map extends Mapper<Object, Text, Text, IntWritable>{
    private Text sortkey = new Text();
    private IntWritable sortval = new IntWritable();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        while(st.hasMoreTokens())
        {
            int num = Integer.parseInt(st.nextToken());
            sortkey.set(num + "");
            sortval.set(num);
            context.write(sortkey,sortval);
        }
    }
}

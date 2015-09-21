import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRSort_Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private List<Integer> unsorted_List = new ArrayList<Integer>();
    private Text sorttext = new Text();


    public void reduce(Text arg0, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        for(IntWritable intvalues : values){
            unsorted_List.add(intvalues.get());
        }
    }

    public void cleanup(Context context)
            throws IOException, InterruptedException {

        Collections.sort(unsorted_List);

        for(Integer sortlist: unsorted_List){
            context.write(new Text(""), new IntWritable(sortlist));
        }
    }
}

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

    public class MRSearch_Map extends Mapper<Object,Text, Text, Text> {

        private String temp;
        private Text csfile = new Text();
        private static int lineindex = 0;

        public void setup(Context context) throws InterruptedException, IOException {
            super.setup(context);
            temp = ((FileSplit) context.getInputSplit()).getPath().toString();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            lineindex++;
            while (st.hasMoreTokens()) {
                csfile.set(temp);
                context.write(csfile, new Text(lineindex + "," + st.nextToken()));
            }
        }
    }

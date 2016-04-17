package cloud.computing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable>
{
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str[] = value.toString().toLowerCase().replaceAll("[^a-z ]", "") .split("\\s+");

        for (String s: str){
            //if (s.equals("harry")) { Question 1b
                word.set(s);
                context.write(word, one);
            //}
        }
    }
}

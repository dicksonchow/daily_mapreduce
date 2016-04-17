package cloud.computing;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EdgeConnectMapper extends Mapper<Object, Text, Text, Text>
{
    private Text keyToPass = new Text();
    private Text valToPass = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split("\\s+");

        if (str.length == 2){
            keyToPass.set(str[0]);
            valToPass.set(str[1]);
            context.write(keyToPass, valToPass);
        }
    }
}

package cloud.computing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class EdgeConnectReducer extends Reducer<Text, Text, Text, IntWritable>
{
    private Map<String, Integer> hm;

    public static Map<String, Integer> sortMapByValue(Map<String, Integer> unsortedMap)
    {
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(unsortedMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });
        Collections.reverse(list);

        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext();) {
            Map.Entry<String, Integer> entry = it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        hm = new HashMap<String, Integer>();
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int count = 0;
        String previous, now;
        previous = "";
        Iterator<Text> it = values.iterator();

        while (it.hasNext()){
            now = it.next().toString();

            if (previous.isEmpty() || !previous.equals(now)){
                previous = now;
                count++;
            }

        }

        hm.put(key.toString(), count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry: sortMapByValue(hm).entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}

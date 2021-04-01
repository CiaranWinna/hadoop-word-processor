
// Importing libraries 
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WCReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    // will hold the output of the reducer
    private TreeMap<Integer, Text> wordMap = new TreeMap<Integer, Text>(Collections.reverseOrder());
    private OutputCollector<Text, IntWritable> collector;

    // Reduce function
    public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> output, Reporter rep)
            throws IOException {
        // assigning the collector
        if (collector == null) {
            collector = output;
        }

        int count = 0;

        // Counting the frequency of each words
        while (value.hasNext()) {
            IntWritable i = value.next();
            count += i.get();
        }

        // adding to the map
        wordMap.put(new Integer(count), new Text(key));

    }

    @Override
    public void close() throws IOException {
        Set<Integer> keys = wordMap.keySet();
        for (Integer k : keys) {
            collector.collect(wordMap.get(k), new IntWritable(k));
        }

    }

}

// Importing libraries 
import java.io.IOException; 

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter;

  
public class WCMapper extends MapReduceBase implements Mapper<LongWritable, 
                                                Text, Text, IntWritable> { 
	
	private int length_of_word = 0;
  
    // Map function 
    public void map(LongWritable key, Text value, OutputCollector<Text,  
                 IntWritable> output, Reporter rep) throws IOException 
    { 
    	
        String line = value.toString().replaceAll("\\p{P}", "").toLowerCase();
  
        // Splitting the line on spaces 
        for (String word : line.split(" "))  
        { 
            if (word.length() == length_of_word) 
            { 
                output.collect(new Text(word), new IntWritable(1)); 
            } 
        } 
    }
    
    public void configure(JobConf job){
    	if(length_of_word == 0){
    		length_of_word = Integer.parseInt(job.get("len"));    	
    	}
    }
} 

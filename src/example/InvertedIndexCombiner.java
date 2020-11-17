package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException{
        int sum = 0;
        for (Text value : values){
            sum += Integer.parseInt(value.toString());
        }
        int splitIndex = key.toString().indexOf(":");
        
        Text word = new Text(key.toString().substring(0,splitIndex));
        Text DocPlusNum = new Text(key.toString().substring(splitIndex+1) + ":" + sum);
        context.write(word, DocPlusNum);
    }
}

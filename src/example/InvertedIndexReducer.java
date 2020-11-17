package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String fileList = new String();
        int docNum = 0;
        int frequencySum = 0;
        for(Text value : values){
            fileList += value.toString() + ";";
            docNum += 1;
            int splitIndex = value.toString().indexOf(":");
            String str = value.toString().substring(splitIndex+1);
            int num = Integer.parseInt(str);
            frequencySum += num;
        }
        context.write(key, new Text(String.format("%.2f",frequencySum/(double)docNum) + "," + fileList));
    }
}

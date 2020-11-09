package example;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();

        Text fileName_lineOffset = new Text(fileName);
        StringTokenizer itr = new StringTokenizer(value.toString());
//        HashMap<String,Integer> frequency = new HashMap<String,Integer>();
        for(; itr.hasMoreTokens(); )
        { word.set(itr.nextToken());
            /*String w = word.toString();
            if(!frequency.containsKey(w)){
                frequency.put(w,1);
            }
            else {
                frequency.put(w,frequency.get(w)+1);
            }*/
            Text wordPlusDoc = new Text(word.toString()+","+fileName);
            context.write(wordPlusDoc, new IntWritable(1));
        }
        /*for (String i : frequency.keySet()) {
            Text keyword = new Text(i);
            Text valueItem = new Text(fileName+"#"+frequency.get(i).toString());
            context.write(keyword, valueItem);
        }*/
    }
}

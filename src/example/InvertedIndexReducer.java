package example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    String prevTerm = new String("");
    int docNum = 0;
    int frequencySum = 0;
    StringBuilder postingList = new StringBuilder();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String currentTerm = key.toString().split(",")[0];
        if(!prevTerm.equals("")&&!currentTerm.equals(prevTerm)){
            context.write(new Text(currentTerm),
                    new Text(String.format("%.2f",frequencySum/(double)docNum)+","+postingList.toString()));
            docNum = 0;
            frequencySum = 0;
            postingList = new StringBuilder();
        }
        int frequency = 0;
        for(IntWritable val:values){
            frequency += val.get();
        }
        String filename=key.toString().split(",")[1].substring(0,key.toString().split(",")[1].lastIndexOf(".txt"));
        postingList.append(filename+":"+frequency+";");
        docNum++;
        frequencySum += frequency;
        prevTerm = currentTerm;

        /*Iterator<Text> it = values.iterator();

        HashMap<String,Integer> docFrequency = new HashMap<String,Integer>();
        for(; it.hasNext(); ){
            String currentDoc = it.next().toString();
            if(!docFrequency.containsKey(currentDoc)){
                docFrequency.put(currentDoc,1);
            }
            else {
                docFrequency.put(currentDoc,docFrequency.get(currentDoc)+1);
            }
        }
        double docNum = 0,frequencySum = 0;
        for (String i : docFrequency.keySet()) {
            all.append(i+":"+docFrequency.get(i)+";");
            docNum++;
            frequencySum+=docFrequency.get(i);
        }
        context.write(key, new Text(String.format("%.2f",frequencySum/docNum)+","+all.toString()));*/
    }
}

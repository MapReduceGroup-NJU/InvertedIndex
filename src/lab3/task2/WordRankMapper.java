package lab3.task2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class WordRankMapper extends Mapper<Object, Text, CountWordKey, IntWritable> {

    // IntWritable is a const value 1.
    private static final IntWritable _one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\n");
        for(int i=0; i<lines.length; ++i){
            String[] tokens = lines[i].split("\t");
            if(tokens.length < 2)
                return;
            String  word = new String(tokens[0]);
            String  countStrWithComma = new String(tokens[1].split(" ")[0]); // e.g., "12.32,".
            String  countStrWithOutComma = countStrWithComma.substring(0, countStrWithComma.length()-1); // "12.32"
            context.write(new CountWordKey(countStrWithOutComma, word), new IntWritable(1));
        }
    }
}

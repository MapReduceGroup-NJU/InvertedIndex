package lab3.task1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {

    // IntWritable is a const value 1.
    private static final IntWritable _one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        // Remove unusable tail of fineName.
        String fileNamePrefix = fileName.substring(0, fileName.length() - ".txt.segmented".length());
        Text word = new Text();
        StringTokenizer tokenReader = new StringTokenizer(value.toString());
        while (tokenReader.hasMoreTokens()) {
            word.set(tokenReader.nextToken() + "#" + fileNamePrefix);
            context.write(word, _one);
        }
    }
}

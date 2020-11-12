package lab3.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        // Only hash on word (not include fileName).
        String word = key.toString().split("#")[0];
        return super.getPartition(new Text(word), value, numReduceTasks);
    }

}

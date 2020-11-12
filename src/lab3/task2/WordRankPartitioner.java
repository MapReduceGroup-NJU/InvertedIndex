package lab3.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordRankPartitioner extends HashPartitioner<CountWordKey, IntWritable> {
    @Override
    public int getPartition(CountWordKey key, IntWritable value, int numReduceTasks) {
        // Only hash on word (not include fileName).
        return key.getWord().hashCode() % numReduceTasks;
    }
}

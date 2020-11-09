package example;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexerPartitioner extends HashPartitioner<Text,IntWritable>
// org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
{ // override the method
    public int getPartition(Text key, IntWritable value, int numReduceTasks)
    {
        Text term =new Text(key.toString().split(",")[0]) ; //<term, docid>=>term
        return super.getPartition(term, value, numReduceTasks);
    }
}

package lab3.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordRanker {
    public static void main(String[] args)
    {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word rank");
            job.setJarByClass(WordRanker.class);
            job.setMapperClass(WordRankMapper.class);
            job.setReducerClass(WordRankReducer.class);
            job.setPartitionerClass(WordRankPartitioner.class); // Set new partitioner.

            job.setMapOutputKeyClass(CountWordKey.class);  // Set new Key.
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace(); }
    }
}

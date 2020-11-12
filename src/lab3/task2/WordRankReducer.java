package lab3.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordRankReducer extends Reducer<CountWordKey, IntWritable, Text, Text> {

    private static Text _EMPRY_WORD_ = new Text();
    private Text m_prevWord;
    private List<String> m_postingList;
    private int  m_wordCnt;  // For all doc.


    @Override
    protected void reduce(CountWordKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text word = new Text(key.getWord());
        Text countAvg = new Text(key.getCount());
        context.write(word, countAvg);
    }

}

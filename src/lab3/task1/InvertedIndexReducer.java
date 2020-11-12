package lab3.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

    private static DecimalFormat _DOUBLE_FORMATER_ = new DecimalFormat("#.00");
    private static Text _EMPRY_WORD_ = new Text();
    private Text m_prevWord;
    private List<String> m_postingList;
    private int  m_wordCnt;  // For all doc.

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        m_prevWord = _EMPRY_WORD_;
        m_postingList = new ArrayList<>();
        m_wordCnt = 0;
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        commitResultOnce(context);
        super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String[] word_file = key.toString().split("#");
        Text word = new Text( word_file[0]) ;
        String story = word_file[1];

        if ( !m_prevWord.equals(word) && !m_postingList.isEmpty() )
        {
            commitResultOnce(context);
            m_prevWord = word;
            m_wordCnt = 0;
            m_postingList.clear();
        }

        // Get Total Cnt of the word in a specific story.
        int wordCntPerDoc = 0;
        for(IntWritable cnt : values)
        {
            wordCntPerDoc += cnt.get();
        }
        m_wordCnt += wordCntPerDoc;
        m_postingList.add(story + ":" + wordCntPerDoc);
    }

    private void commitResultOnce(Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        builder.append(_DOUBLE_FORMATER_.format(m_wordCnt / (double) m_postingList.size()));
        builder.append(", ");
        for (String str : m_postingList)
        {
            builder.append(str);
            builder.append("; ");
        }
        context.write(m_prevWord, new Text( builder.toString() ));
    }

}

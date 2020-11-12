package lab3.task2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountWordKey implements WritableComparable<CountWordKey> {
    private String m_word;
    private Double m_countAvg;

    public CountWordKey(){
    }

    public CountWordKey(String count, String word){
        m_countAvg = Double.parseDouble(count);
        m_word = word;
    }

    public String getWord(){
        return m_word;
    }
    public String getCount(){
        return m_countAvg.toString();
    }

    @Override
    public int compareTo(CountWordKey o) {  // Used by Sorting.
        if (m_countAvg > o.m_countAvg)
            return -1;
        else if (m_countAvg < o.m_countAvg)
            return 1;
        else
            return m_word.compareTo(o.m_word);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException { // Remember it.
        dataOutput.writeUTF(m_countAvg.toString());
        dataOutput.writeUTF(m_word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        m_countAvg = Double.parseDouble(dataInput.readUTF());
        m_word = dataInput.readUTF();
    }

    @Override
    public String toString(){
        return m_countAvg.toString() + "#" + m_word;
    }
}

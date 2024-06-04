package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public enum Counters {
        TOTAL_LETTERS
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        context.getCounter(Counters.TOTAL_LETTERS).increment(sum);
        context.write(new Text(key), new LongWritable(sum));
    }
}




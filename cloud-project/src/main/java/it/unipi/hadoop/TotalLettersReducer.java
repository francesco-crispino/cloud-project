package it.unipi.hadoop;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalLettersReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    public enum Counters {
        TOTAL_LETTERS
    }

    private static AtomicLong sharedCounts = new AtomicLong(0);
    private final static Text key_total_letters = new Text("'total_letters_count'");

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable val : values) {
            sharedCounts.incrementAndGet();
            sum++;
        }

        context.getCounter(Counters.TOTAL_LETTERS).increment(sum);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(key_total_letters, new LongWritable(sharedCounts.get()));
    }
}

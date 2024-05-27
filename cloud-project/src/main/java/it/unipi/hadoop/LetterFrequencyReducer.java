package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    // private long totalLetters = 0; // Contatore totale delle lettere
    private Map<Text, IntWritable> letterSums = new HashMap<>();
    private static Map<Text, AtomicLong> sharedCounts = new HashMap<>();
    private static Long totalLettersCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String totalLettersParameter = conf.get("total.letters.count");
        if (totalLettersParameter != null) {
            totalLettersCount = Long.parseLong(totalLettersParameter);
        } else {
            totalLettersCount = 1L;
        }
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        AtomicLong count = sharedCounts.computeIfAbsent(key, k -> new AtomicLong(0));
        for(IntWritable val: values) {
            count.addAndGet(val.get());
        }
        letterSums.put(new Text(key), new IntWritable((int)count.get()));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        /*for(Text letter: letterSums.keySet()) {
            int sum = letterSums.get(letter).get();
            double frequency = (double) sum / totalLetters;
            context.write(letter, new DoubleWritable(frequency));
        }*/

        long totalLetters = sharedCounts.values().stream().mapToLong(AtomicLong::get).sum();
        for(Text letter: letterSums.keySet()) {
            double sum = (double) letterSums.get(letter).get();
            double frequency = sum / totalLettersCount;
            context.write(letter, new DoubleWritable(frequency));
            //context.write(letter, new DoubleWritable(sum));
        }
    }
}

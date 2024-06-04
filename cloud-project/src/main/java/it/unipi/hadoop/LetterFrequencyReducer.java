package it.unipi.hadoop;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;


public class LetterFrequencyReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

    private long totalLetters = 0; // Contatore totale delle lettere
    private Map<Text, LongWritable> letterSums = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Inizializzazione: totalLetters parte da zero
        Configuration conf = context.getConfiguration();
        String delimiter = conf.get("letterCount", " ");
        totalLetters = Long.parseLong(delimiter);
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable val : values) {
            sum += val.get();
        }
        letterSums.put(new Text(key), new LongWritable(sum));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Text letter: letterSums.keySet()) {
            long sum = letterSums.get(letter).get();
            double frequency = (double) sum / totalLetters;
            context.write(letter, new DoubleWritable(frequency));
        }
    }
}

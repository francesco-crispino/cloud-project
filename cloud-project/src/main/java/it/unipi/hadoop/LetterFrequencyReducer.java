package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private long totalLetters = 0; // Contatore totale delle lettere
    private Map<Text, IntWritable> letterSums = new HashMap<>();

    /*@Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Inizializzazione: totalLetters parte da zero
    }*/

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        letterSums.put(new Text(key), new IntWritable(sum));
        totalLetters += sum;
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Text letter: letterSums.keySet()) {
            int sum = letterSums.get(letter).get();
            double frequency = (double) sum / totalLetters;
            context.write(letter, new DoubleWritable(frequency));
        }
    }
}

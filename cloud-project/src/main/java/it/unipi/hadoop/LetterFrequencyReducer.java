package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private long totalLetters = 0; // Contatore totale delle lettere

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Inizializzazione: totalLetters parte da zero
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
            totalLetters += val.get(); // Aggiorna il conteggio totale
        }
        // Calcola la frequenza solo alla fine, quando totalLetters è completo
        if (totalLetters != 0) { // Evita divisione per zero
            double frequency = (double) sum / totalLetters;
            context.write(key, new DoubleWritable(frequency));
        }
    }
}

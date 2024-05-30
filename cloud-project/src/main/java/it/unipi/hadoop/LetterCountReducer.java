package it.unipi.hadoop;
import java.util.Map;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;

public class LetterCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //private long totalLetters = 0; // Contatore totale delle lettere
    //private Map<Text, IntWritable> letterSums = new HashMap<>();
    private IntWritable result = new IntWritable();
   
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

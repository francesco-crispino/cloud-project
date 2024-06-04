package it.unipi.hadoop;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.Normalizer;

public class LetterCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable word_sum = new LongWritable(1);
    private final static Text charKey = new Text("letter");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        word_sum.set(0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();

        // Normalize removing accents
        String letters = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

        for (int i = 0; i < letters.length(); i++) {
            char c = letters.charAt(i);
            if (Character.isLetter(c)) {
                word_sum.set(word_sum.get()+1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(charKey, word_sum);
    }
}
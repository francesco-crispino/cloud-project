package it.unipi.hadoop;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text letter = new Text();
    private Map<String, Integer> letterSums = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        letterSums = new HashMap<>();
        for (char c = 'a'; c <= 'z'; c++) {
            letterSums.put(String.valueOf(c), 0);
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();

        // Normalize removing accents
        String letters = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

        for (int i = 0; i < letters.length(); i++) {
            char c = letters.charAt(i);
            if (Character.isLetter(c)) {
                letter.set(String.valueOf(c));
                letterSums.put(String.valueOf(c), letterSums.get(String.valueOf(c)) + 1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
        for (Map.Entry<String, Integer> m : letterSums.entrySet()) {
            System.out.println(m.getKey() + " " + m.getValue());
            context.write(new Text(m.getKey()),  new LongWritable(m.getValue()));
        }
    }
}
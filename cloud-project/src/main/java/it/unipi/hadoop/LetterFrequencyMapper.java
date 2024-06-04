package it.unipi.hadoop;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.Normalizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private Text letter = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();

        // Normalize removing accents
        String letters = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

        for (int i = 0; i < letters.length(); i++) {
            char c = letters.charAt(i);
            if (Character.isLetter(c)) {
                letter.set(String.valueOf(c));
                context.write(letter, one);
            }
        }
    }
}
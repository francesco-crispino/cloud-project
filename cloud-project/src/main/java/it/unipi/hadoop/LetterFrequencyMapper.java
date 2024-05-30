package it.unipi.hadoop;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;


public class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text letter = new Text();
    //private HashMap<String, Integer> letterCounts;

    /*
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        letterCounts = new HashMap<>();
        for (char c = 'a'; c <= 'z'; c++) {
            letterCounts.put(String.valueOf(c), 0);
        }
    }
    */
    
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

    /*@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(String letter: letterCounts.keySet()) {
            context.write(new Text(letter), new IntWritable(letterCounts.get(letter)));
        }
    }*/
}
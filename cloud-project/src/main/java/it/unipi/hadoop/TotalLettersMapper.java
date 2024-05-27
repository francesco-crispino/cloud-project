package it.unipi.hadoop;

import java.io.IOException;
import java.text.Normalizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalLettersMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static Text total_letters_key = new Text("total_letters");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();

        // normalize the line by removing accents and leaving only ASCII characters
        String letters = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");

        for (int i = 0; i < letters.length(); i++) {
            char c = letters.charAt(i);
            if (Character.isLetter(c)) {
                // the key is the same for all the letters found in the file
                // because in this mapper we want to find the total number of letters
                context.write(total_letters_key, one);
            }
        }
    }
}
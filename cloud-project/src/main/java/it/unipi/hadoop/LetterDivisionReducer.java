package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class LetterDivisionReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private static Long totalLettersCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        // in the configuration file of this reducer is passed
        // the total number of letters in the files
        Configuration conf = context.getConfiguration();
        String totalLettersParameter = conf.get("total.letters.count");

        if (totalLettersParameter != null) {
            totalLettersCount = Long.parseLong(totalLettersParameter);
        } else {
            // if the parameter is null I initialize it to 1
            // because then a division will be performed
            totalLettersCount = 1L;
        }
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: values) {
            sum += val.get();
        }
        double frequency = (double) sum / totalLettersCount;
        // TODO: cambiare tutti gli int in long perché non si sa mai nei file grandi
        context.write(new Text(key), new DoubleWritable(frequency));
    }
}

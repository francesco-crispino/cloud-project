package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequencyReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(new Text(key), new LongWritable(sum));
    }
}

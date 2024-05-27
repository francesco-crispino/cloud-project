package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LetterDivisionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // each line in the file consists of a letter and the
        // total number of occurrences divided by tab
        String line = value.toString().toLowerCase();
        String[] fields = line.split("\t");

        context.write(new Text(fields[0]), new IntWritable(Integer.parseInt(fields[1])));
    }
}

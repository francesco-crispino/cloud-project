package it.unipi.hadoop;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;


public class LetterFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: letterfrequency <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job1 = Job.getInstance(conf, "letter count");
        job1.setJarByClass(LetterFrequency.class);
        job1.setMapperClass(LetterCountMapper.class);
        job1.setCombinerClass(LetterCountReducer.class); // the combiner and reducer are the same
        job1.setReducerClass(LetterCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1]));
        job1.waitForCompletion(true);


        //Read the letter count from the file
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        // Assuming there is only one output file part-r-00000
        Path outputFile = new Path(outputPath, "part-r-00000");
        FSDataInputStream inputStream = fs.open(outputFile);

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        String number = " ";
        if ((line = reader.readLine()) != null) {
            Pattern pattern = Pattern.compile("\\d+");
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                number = matcher.group();
                
            }
        }
        else{
            System.err.println("No value printed from the job");
            System.exit(0);
        }

        System.out.println("The number is: " + number);


        //lunch the second job that will compute the letter frequency
        conf.set("letterCount",number);
        Job job = Job.getInstance(conf, "letter frequency");
        job.setJarByClass(LetterFrequency.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setCombinerClass(LetterFrequencyCombiner.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

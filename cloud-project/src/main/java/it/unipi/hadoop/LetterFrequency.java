package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

public class LetterFrequency {

    public static void main(String[] args) throws Exception {
        final Integer MAX_NUM_OF_REDUCER = 26;

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: letterfrequency <in> <out_first_job> <out_second_job>");
            System.exit(2);
        }

        String input_file = otherArgs[0];
        String output_dir_first_job = otherArgs[1];
        String output_dir_second_job = otherArgs[2];

        BufferedWriter executionTimesWriter;
        Path execution_times_path = new Path(output_dir_first_job + "/execution_times.txt");
        FSDataOutputStream execution_times_out = fs.create(execution_times_path, true);
        executionTimesWriter = new BufferedWriter(new OutputStreamWriter(execution_times_out, StandardCharsets.UTF_8));
        executionTimesWriter.write("reducers_nums,execution_time");
        executionTimesWriter.newLine();

         for (int reducer_nums = 1; reducer_nums <= MAX_NUM_OF_REDUCER; reducer_nums += 3) {

            // -------- START HADOOP ALGORITHM ---------

            Job job1 = Job.getInstance(conf, "letter count");
            job1.setJarByClass(LetterFrequency.class);
            job1.setMapperClass(LetterCountMapper.class);
            job1.setCombinerClass(LetterCountReducer.class); // the combiner and reducer are the same
            job1.setReducerClass(LetterCountReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(job1, new Path(input_file));

            Path resultsDir = new Path(output_dir_first_job, "total_letters_count" + reducer_nums);
            FileOutputFormat.setOutputPath(job1, resultsDir);

            long startTime = System.currentTimeMillis();
            job1.waitForCompletion(true);

            // open and read the output of the previous file
            Path outputFile = new Path(resultsDir, "part-r-00000");
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

            System.out.println("The total letter count is: " + number);

             //lunch the second job that will compute the letter frequency
            conf.set("letterCount",number);
            Job job2 = Job.getInstance(conf, "letter frequency");
            job2.setJarByClass(LetterFrequency.class);
            job2.setMapperClass(LetterFrequencyMapper.class);
            job2.setCombinerClass(LetterFrequencyCombiner.class);
            job2.setReducerClass(LetterFrequencyReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);
            job2.setNumReduceTasks(reducer_nums);


            FileInputFormat.addInputPath(job2, new Path(input_file));
            
            FileOutputFormat.setOutputPath(job2, new Path(output_dir_second_job));

            resultsDir = new Path(output_dir_second_job, "letter_frequency" + reducer_nums);
            FileOutputFormat.setOutputPath(job2, resultsDir);

             job2.waitForCompletion(true);
            // -------- END HADOOP ALGORITHM ---------

             long endTime = System.currentTimeMillis();
            System.out.println("Job with " + reducer_nums + " reducers took " + (endTime - startTime) + " milliseconds");
            executionTimesWriter.write(reducer_nums + "," + (endTime - startTime));
            executionTimesWriter.newLine();
            if(reducer_nums == 25) reducer_nums = 23; // this one to have 26 reducer as the number of letters
        }
        executionTimesWriter.close();
    }
}

package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class LetterFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: letterfrequency <in> [<in>...] <out>");
            System.exit(2);
        }

        BufferedWriter executionTimesWriter = null;
        Path execution_times_path = new Path(args[args.length - 1] + "/execution_times.txt");
        FSDataOutputStream execution_times_out = fs.create(execution_times_path, true);
        executionTimesWriter = new BufferedWriter(new OutputStreamWriter(execution_times_out, StandardCharsets.UTF_8));
        executionTimesWriter.write("reducers_nums,execution_time");
        executionTimesWriter.newLine();

        for (int reducer_nums = 1; reducer_nums <= 26; reducer_nums += 3) {
            // the first job is used to calculate the total number of letters in the files
            Job job1 = Job.getInstance(conf, "total number of letters with " + reducer_nums + " reducers");
            job1.setJarByClass(LetterFrequency.class);
            job1.setMapperClass(LetterFrequencyMapper.class);
            job1.setCombinerClass(LetterFrequencyCombiner.class);
            job1.setReducerClass(TotalLettersReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.setNumReduceTasks(reducer_nums);

            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
            }

            Path resultsDir = new Path(otherArgs[otherArgs.length - 1], "total_letters_count" + reducer_nums);
            FileOutputFormat.setOutputPath(job1, resultsDir);

            long startTime = System.currentTimeMillis();
            boolean job1Completed = job1.waitForCompletion(true);
            if (!job1Completed) {
                System.out.println("[ERROR] An error occurred during the execution of the first job");
                System.exit(1);
            }

            FileStatus[] fileStatuses = fs.listStatus(resultsDir);

            // take the value of the reducer counter that contains the value of
            // the total number of letters in the files
            long totalLettersCount = job1.getCounters()
                    .findCounter(TotalLettersReducer.Counters.TOTAL_LETTERS)
                    .getValue();
            System.out.println("[INFO] Total letters: " + totalLettersCount);

            // the second job is used to calculate the frequency of individual letters
            conf.set("total.letters.count", String.valueOf(totalLettersCount));

            Job job2 = Job.getInstance(conf, "letters frequency");
            job2.setJarByClass(LetterFrequency.class);
            job2.setMapperClass(LetterDivisionMapper.class);
            job2.setReducerClass(LetterDivisionReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            // FIXME: per ora lo lascio di default poi vedere se impostare anche i suoi reducer

            // take all the files in the resultsDir folder
            // and give them as input to the second job
            for (FileStatus status : fileStatuses) {
                Path filePath = status.getPath();
                if (filePath.getName().startsWith("part-r-")) {
                    FileInputFormat.addInputPath(job2, filePath);
                }
            }

            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1], "letters_frequency" + reducer_nums));

            boolean job2Completed = job2.waitForCompletion(true);
            if (!job2Completed) {
                System.out.println("[ERROR] An error occurred during the execution of the second job");
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Job with " + reducer_nums + " reducers took " + (endTime - startTime) + " milliseconds");
            executionTimesWriter.write(reducer_nums + "," + (endTime - startTime));
            executionTimesWriter.newLine();
        }
        executionTimesWriter.close();
        IOUtils.closeStream(executionTimesWriter);
    }
}

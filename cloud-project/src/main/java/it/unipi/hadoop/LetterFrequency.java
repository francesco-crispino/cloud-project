package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * The second version of the program implements a MapReduce-based approach to
 * calculate the frequency of each letter within a set of files.
 * Process Steps:
 * 1. Mapper:
 *  - Reads each line of the input file.
 *  - For each letter in the line:
 *      - Uses the letter as a key and "1" as a value.
 *      - Increments a counter to record the number of occurrences of the letter.
 * 2. Combiner:
 *  - Sums the values with the same key (letter) received from the mappers.
 * 3. Reducer:
 *  - For each key-value pair received from the combiner (letter, sum_occurrences):
 *      - Writes to a file the pair formed by: letter, a tab ("\t"), and the sum of the occurrences.
 * 4. Main:
 *  - Retrieves the total letter count from the "TOTAL_LETTERS" counter used in the mapper.
 *  - Opens all files named "part-r-*" in the project directory.
 *  - For each line in these files (format: letter "\t" occurrences):
 *  - Calculates the frequency by dividing the occurrences by the total letter count.
 *  - Writes the letter-frequency pair to a new file called "letters_frequency.txt".
 *  A Counter is an object that stores and updates the count of distinct elements.
 *  In this case, the "TOTAL_LETTERS" counter keeps track of the total number of
 *  letters present in all input files.
 */
public class LetterFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: letterfrequency <in> [<in>...] <out>");
            System.exit(2);
        }

        // creation of a buffer for writing data to file to create a csv consisting
        // of rows in "reducers_nums,execution_time" format
        BufferedWriter executionTimesWriter = null;
        Path execution_times_path = new Path(args[args.length - 1] + "/execution_times.txt");
        FSDataOutputStream execution_times_out = fs.create(execution_times_path, true);
        executionTimesWriter = new BufferedWriter(new OutputStreamWriter(execution_times_out, StandardCharsets.UTF_8));
        executionTimesWriter.write("reducers_nums,execution_time");
        executionTimesWriter.newLine();

        // this for is needed to evaluate the performance of the system as the reducers vary,
        // so we start with 1 reducer and work up to 26 (as many as the number of letters)
        for (int reducer_nums = 1; reducer_nums <= 26; reducer_nums += 3) {
            // the first job is used to calculate the total number of letters in the files
            Job job = Job.getInstance(conf, "total number of letters with " + reducer_nums + " reducers");
            job.setJarByClass(LetterFrequency.class);
            job.setMapperClass(LetterFrequencyMapper.class);
            job.setReducerClass(LetterFrequencyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(reducer_nums);

            for (int i = 0; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }

            Path resultsDir = new Path(otherArgs[otherArgs.length - 1], "total_letters_count" + reducer_nums);
            FileOutputFormat.setOutputPath(job, resultsDir);

            // performance in terms of execution time is measured from the moment the job starts
            long startTime = System.currentTimeMillis();
            boolean jobCompleted = job.waitForCompletion(true);

            if (!jobCompleted) {
                System.out.println("[ERROR] An error occurred during the execution of the first job");
                System.exit(1);
            }

            // take the value of the reducer counter that contains the value of
            // the total number of letters in the files
            long totalLettersCount = job.getCounters()
                    .findCounter(LetterFrequencyReducer.Counters.TOTAL_LETTERS)
                    .getValue();

            // there is a need for two buffers, the first to read from the files produced by
            // the reducers and the second to write to the results file.
            // So at this point we have 3 buffers:
            // 1. executionTimesWriter: to write inside the csv to evaluate the performance
            // 2. br: to read from the files produced by the reducers that starts with "part-r-"
            // 3. bw: to write to the letters_frequency.txt file that will contain the letter-frequency pairs
            FileStatus[] fileStatuses = fs.listStatus(resultsDir);
            BufferedReader br = null;
            BufferedWriter bw = null;
            Path letters_frequency_path = new Path(args[args.length - 1] + "/letters_frequency"+ reducer_nums + ".txt");

            try {
                FSDataOutputStream out = fs.create(letters_frequency_path, true);
                bw = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));

                // for each file in the resultDir beginning with part-r-,
                // take the rows and save them in the br buffer
                for (FileStatus status : fileStatuses) {
                    Path filePath = status.getPath();
                    if (filePath.getName().startsWith("part-r-")) {
                        br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
                        String line;

                        // values within a row are divided by tabs,
                        // the key is a string and the value a long
                        while ((line = br.readLine()) != null) {
                            String[] fields = line.split("\t");
                            String key = fields[0];
                            long value = Long.parseLong(fields[1]);

                            // for each pair (row in the file), the frequency is calculated
                            // by dividing the value found in the file by the counter TOTAL_LETTERS
                            double frequency = (double) value / totalLettersCount;

                            // finally, save the result obtained in the letters_frequency.txt file
                            bw.write(key + "\t" + frequency);
                            bw.newLine();
                        }
                        br.close();
                    }
                }
                bw.close();
            } catch (IOException e) {
                System.out.println("[ERROR] an error occurred during the file scan");
                IOUtils.closeStream(executionTimesWriter);
            } finally {
                IOUtils.closeStream(br);
                IOUtils.closeStream(bw);
            }

            // the algorithm is finished so the timer that was started when
            // the job was launched can be stopped, and the results in the csv could be saved
            long endTime = System.currentTimeMillis();
            System.out.println("Job with " + reducer_nums + " reducers took " + (endTime - startTime) + " milliseconds");
            executionTimesWriter.write(reducer_nums + "," + (endTime - startTime));
            executionTimesWriter.newLine();
        }
        executionTimesWriter.close();
        IOUtils.closeStream(executionTimesWriter);
    }
}

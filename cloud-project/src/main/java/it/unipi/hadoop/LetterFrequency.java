package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
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

        // the first job is used to calculate the total number of letters in the files
        Job job = Job.getInstance(conf, "total number of letters");
        job.setJarByClass(LetterFrequency.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setCombinerClass(LetterFrequencyCombiner.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        Path resultsDir = new Path(otherArgs[otherArgs.length - 1], "total_letters_count");
        FileOutputFormat.setOutputPath(job, resultsDir);


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
        System.out.println("[INFO] Total letters: " + totalLettersCount);

        FileStatus[] fileStatuses = fs.listStatus(resultsDir);
        BufferedReader br = null;
        BufferedWriter bw = null;
        Path letters_frequency_path = new Path(args[args.length - 1] + "/letters_frequency.txt");

        try {
            FSDataOutputStream out = fs.create(letters_frequency_path, true);
            bw = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));

            // for each file in the resultDir beginning with part-r-,
            // take the rows and save them in the buffer
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

                        System.out.println("[INFO] (" + key + ", " + value + ")");
                        double frequency = (double) value / totalLettersCount;
                        System.out.println("[INFO] " + key + ", " + frequency);
                        bw.write(key + "\t" + frequency);
                        bw.newLine();
                    }
                    br.close();
                }
            }
            bw.close();
        } finally {
            IOUtils.closeStream(br);
            IOUtils.closeStream(bw);
        }
    }
}

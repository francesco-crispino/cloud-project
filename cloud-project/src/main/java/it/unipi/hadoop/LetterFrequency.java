package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;

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
        Job job1 = Job.getInstance(conf, "total number of letters");
        job1.setJarByClass(LetterFrequency.class);
        job1.setMapperClass(LetterFrequencyMapper.class);
        job1.setCombinerClass(LetterFrequencyCombiner.class);
        job1.setReducerClass(TotalLettersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }

        Path resultsDir = new Path(otherArgs[otherArgs.length - 1], "total_letters_count");
        FileOutputFormat.setOutputPath(job1, resultsDir);


        boolean job1Completed = job1.waitForCompletion(true);
        if (!job1Completed) {
            System.out.println("[ERROR] An error occurred during the execution of the first job");
            System.exit(1);
        }

        FileStatus[] fileStatuses = fs.listStatus(resultsDir);
        BufferedReader br = null;

        /*try {
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
                    }
                    br.close();
                }
            }
        } finally {
            IOUtils.closeStream(br);
        }*/

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

        // take all the files in the resultsDir folder
        // and give them as input to the second job
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            if (filePath.getName().startsWith("part-r-")) {
                FileInputFormat.addInputPath(job2, filePath);
            }
        }

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1], "letters_frequency"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

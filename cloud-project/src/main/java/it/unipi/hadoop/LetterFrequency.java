package it.unipi.hadoop;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LetterFrequency {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: letterfrequency <in> [<in>...] <out>");
            System.exit(2);
        }

        // the first job is used to calculate the total number of letters in the files
        Job job1 = Job.getInstance(conf, "total number of letters");
        job1.setJarByClass(LetterFrequency.class);
        job1.setMapperClass(TotalLettersMapper.class);
        job1.setReducerClass(TotalLettersReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }

        Path total_letters_path = new Path(otherArgs[otherArgs.length - 1], "total_letters_count");
        FileOutputFormat.setOutputPath(job1, total_letters_path);


        boolean job1Completed = job1.waitForCompletion(true);
        if (!job1Completed) {
            System.out.println("[ERROR] An error occurred during the execution of the first job");
            System.exit(1);
        }

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
        job2.setMapperClass(LetterFrequencyMapper.class);
        job2.setCombinerClass(LetterFrequencyCombiner.class);
        job2.setReducerClass(LetterFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job2, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1], "letters_frequency"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

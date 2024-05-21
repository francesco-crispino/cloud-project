import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text letter = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Inizializzazioni opzionali (ad esempio, caricamento di dizionari)
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase(); // Converti in minuscolo per uniformità
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (Character.isLetter(c)) { // Considera solo lettere valide
                letter.set(String.valueOf(c));
                context.write(letter, one); // Invio al combiner/reducer
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Operazioni finali (ad esempio, scrittura di log)
    }
}

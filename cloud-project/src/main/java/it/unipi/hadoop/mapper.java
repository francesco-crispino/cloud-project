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
        String line = value.toString().toLowerCase(); // Converti in minuscolo
        
        // Normalizza e rimuovi gli accenti
        String normalized = Normalizer.normalize(line, Normalizer.Form.NFD);
        String ascii = normalized.replaceAll("[^\\p{ASCII}]", "");

        for (int i = 0; i < ascii.length(); i++) {
            char c = ascii.charAt(i);
            if (Character.isLetter(c)) { 
                letter.set(String.valueOf(c));
                context.write(letter, one); 
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Operazioni finali (ad esempio, scrittura di log)
    }
}

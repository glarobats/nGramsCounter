package nGrams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class nGramsCounter {

    // Ρυθμίζει το μέγεθος των n-grams στο configuration file (ορίζετε εσείς το μέγεθος)
    static int nGramsSize = 2;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("nGrams",nGramsSize); // Προεπιλεγμένο μέγεθος των n-grams
        Job job = new Job(conf, "nGramCount");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();
        private int n;

        // Αναλύει το κείμενο και δημιουργεί τα n-grams
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Μετατροπή του κειμένου σε πεζά
            String line = value.toString().toLowerCase();
            // Αφαίρεση των σημείων στίξης
            line = line.replaceAll("[^a-zA-Z\\s]", "");
            // Χωρίζει το κείμενο σε λέξεις
            StringTokenizer tokenizer = new StringTokenizer(line);

            // Αποθηκεύει τις λέξεις σε μια λίστα
            List<String> words = new ArrayList<>();
            // Προσθέτει τις λέξεις στη λίστα
            while (tokenizer.hasMoreTokens()) {
                words.add(tokenizer.nextToken());
            }

            // Δημιουργεί τα n-grams
            for (int i = 0; i <= words.size() - n; i++) {
                // Αποθηκεύει το n-gram σε ένα StringBuilder
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < n; j++) {
                    // Προσθέτει τις λέξεις στο StringBuilder
                    sb.append(words.get(i + j)).append(" ");
                }

                // Αποθηκεύει το n-gram στο Text
                ngram.set(sb.toString());
                // Επιστρέφει το n-gram και το 1
                context.write(ngram, one);
            }
        }

        // Ρυθμίζει το μέγεθος των n-grams από το configuration file
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("nGrams", nGramsSize); // Προεπιλεγμένο μέγεθος των n-grams
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            // Αθροίζει τα n-grams
            for (IntWritable val : values)
                sum += val.get();

            // Επιστρέφει το n-gram και το άθροισμα
            context.write(key, new IntWritable(sum));
        }
    }
}

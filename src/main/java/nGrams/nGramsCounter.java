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

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ngram = new Text();
        private int n;

        // Ρυθμίζει το μέγεθος των n-grams από το command line
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = conf.getInt("n", 3); // Προεπιλεγμένο μέγεθος των n-grams
        }



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
                // Αφαιρεί το κενό στο τέλος
                //  sb.setLength(sb.length() - 1);
                // Αποθηκεύει το n-gram στο Text
                ngram.set(sb.toString());
                // Επιστρέφει το n-gram και το 1
                context.write(ngram, one);
            }
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Ρυθμίζει το μέγεθος των n-grams από το command line ή απο το configuration file
        conf.setInt("nGrams", Integer.parseInt(args[2]));
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
}

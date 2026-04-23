package pjbl.exe1;

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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransacoesBrazil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration bra = new Configuration();
        String[] files = new GenericOptionsParser(bra, args).getRemainingArgs();

        // entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // saida (PASTA, não arquivo)
        Path output = new Path("out_exe1");

        Job j = new Job(bra, "exe1");

        j.setJarByClass(TransacoesBrazil.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ================= MAP =================
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text brazil = new Text("Brazil");

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Ignorar cabeçalho
            if (line.contains("country_or_area")) return;

            String[] campos = line.split(";", -1);

            // coreções de validação de valores
            if (campos.length < 1 || campos[0].isEmpty()) return;

            String country = campos[0].replace("\"", "").trim();

            if (country.equalsIgnoreCase("Brazil")) {
                con.write(brazil, one);
            }
        }
    }

    // ================= REDUCE =================
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;

            for (IntWritable val : values) {
                soma += val.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }
}
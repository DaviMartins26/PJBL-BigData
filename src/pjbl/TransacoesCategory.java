package pjbl;

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

public class TransacoesCategory {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // saida (PASTA, não arquivo)
        Path output = new Path("out_TCategory");

        Job j = new Job(c, "TCategoty");

        j.setJarByClass(TransacoesCategory.class);
        j.setMapperClass(MapCategoty.class);
        j.setReducerClass(ReduceCategoria.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ================= MAP =================
    public static class MapCategoty extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text categoty = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.contains("country_or_area")) return;

            String[] campos = line.split(";");

            if (campos.length < 10) return;

            String categoria = campos[9];

            categoty.set(categoria);
            context.write(categoty, one);
        }
    }

    // ================= REDUCE =================
    public static class ReduceCategoria extends Reducer<Text, IntWritable, Text, IntWritable> {

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
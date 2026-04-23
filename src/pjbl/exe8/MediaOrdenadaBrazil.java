package pjbl.exe8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MediaOrdenadaBrazil {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path intermediate = new Path("out_exe8_intermediate");
        Path output = new Path("out_exe8_final");

        // ======================
        // JOB 1 - MÉDIA
        // ======================
        Job j1 = new Job(c, "media-por-ano");

        j1.setJarByClass(MediaOrdenadaBrazil.class);

        j1.setMapperClass(MapEtapa1.class);
        j1.setCombinerClass(CombinerEtapa1.class);
        j1.setReducerClass(ReduceEtapa1.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(MediaWritable.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // ======================
        // EXECUTA JOB 1
        // ======================
        if (j1.waitForCompletion(true)) {

            // ======================
            // JOB 2 - ORDENAÇÃO
            // ======================
            Job j2 = new Job(c, "ordenacao");

            j2.setJarByClass(MediaOrdenadaBrazil.class);

            j2.setMapperClass(MapEtapa2.class);
            j2.setReducerClass(ReduceEtapa2.class);

            j2.setMapOutputKeyClass(DoubleWritable.class);
            j2.setMapOutputValueClass(Text.class);

            j2.setOutputKeyClass(DoubleWritable.class);
            j2.setOutputValueClass(Text.class);

            // 🔥 ordem decrescente
            j2.setSortComparatorClass(DoubleWritable.Comparator.class);

            FileInputFormat.addInputPath(j2, intermediate);
            FileOutputFormat.setOutputPath(j2, output);

            j2.waitForCompletion(true);
        }
    }

    // ======================
    // MAP 1
    // ======================
    public static class MapEtapa1 extends Mapper<LongWritable, Text, Text, MediaWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // ignorar cabeçalho
            if (linha.contains("country_or_area")) return;

            String[] campos = linha.split(";");

            if (campos.length < 10) return;

            String country = campos[0];
            String year = campos[1];
            String flow = campos[4];

            double trade;

            try {
                trade = Double.parseDouble(campos[5]);
            } catch (Exception e) {
                return;
            }

            if (country.equals("Brazil") && flow.equals("Export")) {
                con.write(new Text(year), new MediaWritable(trade, 1));
            }
        }
    }

    // ======================
    // COMBINER
    // ======================
    public static class CombinerEtapa1 extends Reducer<Text, MediaWritable, Text, MediaWritable> {

        public void reduce(Text key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int count = 0;

            for (MediaWritable v : values) {
                soma += v.getSoma();
                count += v.getCount();
            }

            con.write(key, new MediaWritable(soma, count));
        }
    }

    // ======================
    // REDUCE 1
    // ======================
    public static class ReduceEtapa1 extends Reducer<Text, MediaWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<MediaWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int count = 0;

            for (MediaWritable v : values) {
                soma += v.getSoma();
                count += v.getCount();
            }

            double media = soma / count;

            con.write(key, new DoubleWritable(media));
        }
    }

    // ======================
    // MAP 2 (INVERTE)
    // ======================
    public static class MapEtapa2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // entrada: 2019\t12345.67

            String[] partes = value.toString().split("\t");

            String ano = partes[0];
            double media; // protecão contra dado não double
            try {
                media = Double.parseDouble(partes[1]);
            } catch (Exception e) {
                return;
            }

            //Menor pra maior com defeitos
            con.write(new DoubleWritable(media), new Text(ano));
            //Maior pra menor, com defeito tambem
            //con.write(new DoubleWritable(-media), new Text(ano));
        }
    }

    // ======================
    // REDUCE 2
    // ======================
    public static class ReduceEtapa2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

        public void reduce(DoubleWritable key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            for (Text ano : values) {
                // menor pra maior com defeito
                con.write(key, ano);
                //maior pra menor com defeito
                //con.write(new DoubleWritable(-key.get()), ano);
            }
        }
    }
}
package pjbl.exe8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MaxOrdenadoBrazil {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path intermediate = new Path("out_exe8_intermediate");
        Path output = new Path("out_exe8_final");

        Job j1 = new Job(c, "max-por-ano");

        j1.setJarByClass(MaxOrdenadoBrazil.class);

        j1.setMapperClass(MapEtapa1.class);
        j1.setCombinerClass(CombinerEtapa1.class);
        j1.setReducerClass(ReduceEtapa1.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        //job 1
        if (j1.waitForCompletion(true)) {

            // JOB 2 = ordenacao
            Job j2 = new Job(c, "ordenacao");

            j2.setJarByClass(MaxOrdenadoBrazil.class);

            j2.setMapperClass(MapEtapa2.class);
            j2.setReducerClass(ReduceEtapa2.class);

            // 🔥 chave agora é customizada
            j2.setMapOutputKeyClass(ValorAnoWritable.class);
            j2.setMapOutputValueClass(NullWritable.class);

            j2.setOutputKeyClass(DoubleWritable.class);
            j2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(j2, intermediate);
            FileOutputFormat.setOutputPath(j2, output);

            j2.waitForCompletion(true);
        }
    }

    // ====================== MAP 1 ======================
    public static class MapEtapa1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (linha.contains("country_or_area")) return;

            String[] campos = linha.split(";");

            // verificação de campo
            if (campos.length < 10) return;
            if (campos[0].isEmpty() || campos[1].isEmpty() || campos[5].isEmpty()) return;

            String country = campos[0].trim();
            String year = campos[1];

            double trade;

            try {
                trade = Double.parseDouble(campos[5]);
            } catch (Exception e) {
                return;
            }

            if (country.equalsIgnoreCase("Brazil")) {
                con.write(new Text(year), new DoubleWritable(trade));
            }
        }
    }

    // ======================COMBINER 1======================
    public static class CombinerEtapa1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;

            for (DoubleWritable v : values) {
                if (v.get() > max) {
                    max = v.get();
                }
            }

            con.write(key, new DoubleWritable(max));
        }
    }

    // ===================== REDUCE 1
    public static class ReduceEtapa1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = Double.MIN_VALUE;

            for (DoubleWritable v : values) {
                if (v.get() > max) {
                    max = v.get();
                }
            }

            con.write(key, new DoubleWritable(max));
        }
    }

    // ===================MAP 2 =====================
    public static class MapEtapa2 extends Mapper<LongWritable, Text, ValorAnoWritable, NullWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] partes = value.toString().split("\t");

            if (partes.length < 2) return;

            String ano = partes[0];

            double valor;

            try {
                valor = Double.parseDouble(partes[1]);
            } catch (Exception e) {
                return;
            }

            con.write(new ValorAnoWritable(valor, ano), NullWritable.get());
        }
    }

    // ====================== REDUCE 2 ======================
    public static class ReduceEtapa2 extends Reducer<ValorAnoWritable, NullWritable, DoubleWritable, Text> {

        public void reduce(ValorAnoWritable key, Iterable<NullWritable> values, Context con)
                throws IOException, InterruptedException {

            con.write(new DoubleWritable(key.getValor()), new Text(key.getAno()));
        }
    }
}
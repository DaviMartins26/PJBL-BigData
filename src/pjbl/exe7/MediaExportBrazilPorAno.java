package pjbl.exe7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MediaExportBrazilPorAno {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("out_exe7");

        Job j = new Job(c, "Media Export Brazil Por Ano");

        j.setJarByClass(MediaExportBrazilPorAno.class);

        j.setMapperClass(MapClass.class);
        j.setCombinerClass(CombinerClass.class);
        j.setReducerClass(ReduceClass.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ExportWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ================= MAP =================
    public static class MapClass extends Mapper<LongWritable, Text, Text, ExportWritable> {

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

            // filtros
            if (country.equals("Brazil") && flow.equals("Export")) {
                con.write(new Text(year), new ExportWritable(trade, 1));
            }
        }
    }

    // ================= COMBINER =================
    public static class CombinerClass extends Reducer<Text, ExportWritable, Text, ExportWritable> {

        public void reduce(Text key, Iterable<ExportWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            int count = 0;

            for (ExportWritable v : values) {
                soma += v.getSoma();
                count += v.getCount();
            }

            con.write(key, new ExportWritable(soma, count));
        }
    }

    // ================= REDUCE =================
    public static class ReduceClass extends Reducer<Text, ExportWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<ExportWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaTotal = 0;
            int countTotal = 0;

            for (ExportWritable v : values) {
                somaTotal += v.getSoma();
                countTotal += v.getCount();
            }

            double media = somaTotal / countTotal;

            con.write(key, new DoubleWritable(media));
        }
    }
}
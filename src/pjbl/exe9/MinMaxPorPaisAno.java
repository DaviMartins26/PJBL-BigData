package pjbl.exe9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MinMaxPorPaisAno {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("out_exe9");

        Job j = new Job(c, "min-max-por-pais-ano");

        j.setJarByClass(MinMaxPorPaisAno.class);

        j.setMapperClass(MapClass.class);
        j.setCombinerClass(CombinerClass.class);
        j.setReducerClass(ReduceClass.class);

        // Reestruturacao do objeto
        j.setMapOutputKeyClass(CountryYearWritable.class);
        j.setMapOutputValueClass(MinMaxWritable.class);

        j.setOutputKeyClass(CountryYearWritable.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ================= MAP =================
    public static class MapClass extends Mapper<LongWritable, Text, CountryYearWritable, MinMaxWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // remover cabeçalho
            if (linha.startsWith("country_or_area")) return;

            String[] campos = linha.split(";");

            if (campos.length < 10) return;

            String country = campos[0];
            int year;

            //validação de campos não vazios
            try {
                year = Integer.parseInt(campos[1]);
            } catch (Exception e) {
                return;
            }

            double trade;

            try {
                trade = Double.parseDouble(campos[5]);
            } catch (Exception e) {
                return;
            }

            CountryYearWritable keyOut = new CountryYearWritable(country, year);
            MinMaxWritable valueOut = new MinMaxWritable(trade, trade);

            con.write(keyOut, valueOut);
        }
    }

    // ================= COMBINER =================
    public static class CombinerClass extends Reducer<CountryYearWritable, MinMaxWritable, CountryYearWritable, MinMaxWritable> {

        public void reduce(CountryYearWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (MinMaxWritable v : values) {
                if (v.getMin() < min) min = v.getMin();
                if (v.getMax() > max) max = v.getMax();
            }

            con.write(key, new MinMaxWritable(min, max));
        }
    }

    // ================= REDUCE =================
    public static class ReduceClass extends Reducer<CountryYearWritable, MinMaxWritable, CountryYearWritable, Text> {

        public void reduce(CountryYearWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (MinMaxWritable v : values) {
                if (v.getMin() < min) min = v.getMin();
                if (v.getMax() > max) max = v.getMax();
            }

            con.write(key, new Text("Min: " + min + "\tMax: " + max));
        }
    }
}
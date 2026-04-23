package pjbl.exe6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class TransacoesMinMaxBrazil2016 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        new GenericOptionsParser(c, args);

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("out_exe6");

        Job j = new Job(c, "exe6");

        j.setJarByClass(TransacoesMinMaxBrazil2016.class);
        j.setMapperClass(MapMinMax.class);
        j.setCombinerClass(CombineMinMax.class);
        j.setReducerClass(ReduceMinMax.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MinMaxWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ===================MAPPER==================
    public static class MapMinMax extends Mapper<LongWritable, Text, Text, MinMaxWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            if (linha.contains("country_or_area")) return;

            String[] campos = linha.split(";");

            if (campos.length < 10) return;

            String country = campos[0];
            String year = campos[1];

            if (!country.equalsIgnoreCase("Brazil")) return;
            if (!year.equals("2016")) return;

            //parseDouble converte String em double
            double valor = Double.parseDouble(campos[5]);

            //con.write(new Text("Brazil-2016"), new MinMaxWritable(valor, valor));
            con.write(new Text(year),new MinMaxWritable(valor,valor));
        }
    }

    //===============COMBINER======================
    public static class CombineMinMax extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

        public void reduce(Text key, Iterable<MinMaxWritable> values, Context con)
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

    //===================Reducer===================
    public static class ReduceMinMax extends Reducer<Text, MinMaxWritable, Text, Text> {

        public void reduce(Text key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (MinMaxWritable v : values) {
                if (v.getMin() < min) min = v.getMin();
                if (v.getMax() > max) max = v.getMax();
            }

            con.write(key, new Text("Min: " + min + " | Max: " + max));
        }
    }
}

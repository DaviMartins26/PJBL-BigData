package pjbl.exe5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TransacoesMediaBrazil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // saída
        Path output = new Path("out_exe5");

        Job j = new Job(c, "exe5");

        j.setJarByClass(TransacoesMediaBrazil.class);
        j.setMapperClass(MapMedia.class);
        j.setReducerClass(ReduceMedia.class);

        // saída do mapper
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(MediaWritable.class);

        // saída final
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // ================= MAP =================
    public static class MapMedia extends Mapper<LongWritable, Text, Text, MediaWritable> {

        private Text ano = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // ignorar cabeçalho
            if (line.contains("country_or_area")) return;

            String[] campos = line.split(";");

            if (campos.length < 10) return;

            String country = campos[0];
            String year = campos[1];

            //Não vai contar qualquer String que não seja "Brazil", não sei se pode fazer asssim.
            if (country.equalsIgnoreCase("Brazil")) {

                try {
                    double valor = Double.parseDouble(campos[5]); // não sei se essa é o tipo correto pra salvar

                    ano.set(year);
                    context.write(ano, new MediaWritable(valor, 1));

                } catch (NumberFormatException e) {
                    // ignora linha inválida
                }
            }
        }
    }

    // ================= REDUCE =================
    public static class ReduceMedia extends Reducer<Text, MediaWritable, Text, Text> {

        public void reduce(Text key, Iterable<MediaWritable> values, Context context)
                throws IOException, InterruptedException {

            double soma = 0;
            int count = 0;

            for (MediaWritable val : values) {
                soma += val.getSoma();
                count += val.getCount();
            }

            double media = soma / count;

            context.write(key, new Text(String.valueOf(media)));
        }
    }
}
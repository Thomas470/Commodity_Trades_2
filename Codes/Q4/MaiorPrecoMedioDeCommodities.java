package RECUPERAÇÃO.Q4;

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

public class MaiorPrecoMedioDeCommodities {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output.rec/ex4.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Criando o primeiro job
        Job j1 = new Job(c, "media1");
        j1.setJarByClass(MaiorPrecoMedioDeCommodities.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceA.class);
        j1.setCombinerClass(CombineA.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(MaiorPrecoMedio_Valor_DeCommoditiesWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "media2");
        j2.setJarByClass(MaiorPrecoMedioDeCommodities.class);
        j2.setMapperClass(MapB.class);
        j2.setReducerClass(ReduceB.class);
        j2.setCombinerClass(CombineEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(MaiorPrecoMedio_Chave_DeCommoditiesWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(MaiorPrecoMedio_Chave_DeCommoditiesWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);


        j2.waitForCompletion(false);


    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, MaiorPrecoMedio_Valor_DeCommoditiesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtendo a linha
            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                String flow = colunas[4];

                // checando se o flow é Export
                if (flow.equals("Export")) {

                    // chave (pais)
                    String pais = colunas[0];

                    // valores (valor e qtd)
                    double valor = Double.parseDouble(colunas[5]);
                    int qtd = 1;

                    MaiorPrecoMedio_Valor_DeCommoditiesWritable valores = new MaiorPrecoMedio_Valor_DeCommoditiesWritable(valor, qtd);

                    // chave e valor
                    con.write(new Text(pais), valores);
                }
            }
        }
    }

    public static class CombineA extends Reducer<Text, MaiorPrecoMedio_Valor_DeCommoditiesWritable, Text, MaiorPrecoMedio_Valor_DeCommoditiesWritable>{

        public void reduce(Text key, Iterable<MaiorPrecoMedio_Valor_DeCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException {

            double somaVals = 0.0;
            int somaQtds = 0;

            // somando os valores e as qtds
            for (MaiorPrecoMedio_Valor_DeCommoditiesWritable o : values) {
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            // mandando para o reduce os valores pré-somados
            con.write(key, new MaiorPrecoMedio_Valor_DeCommoditiesWritable(somaVals, somaQtds));

        }
    }

    public static class ReduceA extends Reducer<Text, MaiorPrecoMedio_Valor_DeCommoditiesWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<MaiorPrecoMedio_Valor_DeCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException {

            // somando os valores e as qtds
            double somaVals = 0.0;
            int somaQtds = 0;

            // somando os valores e as qtds
            for (MaiorPrecoMedio_Valor_DeCommoditiesWritable o : values) {
                somaVals += o.getSomaValores();
                somaQtds += o.getQtd();
            }

            // calculando a media
            double media = somaVals / somaQtds;

            // chave e valor
            con.write(key, new DoubleWritable(media));
        }
    }


    public static class MapB extends Mapper<LongWritable, Text, Text, MaiorPrecoMedio_Chave_DeCommoditiesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Pegando uma linha
            String linha = value.toString();

            // quebrando a linha por tabs
            String linhas[] = linha.split("\t");

            // valor
            String pais = linhas[0];
            double qtd = Double.parseDouble(linhas[1]);

            // chave
            Text chave = new Text("Pais com maior preço médio de commodities");

            MaiorPrecoMedio_Chave_DeCommoditiesWritable valor = new MaiorPrecoMedio_Chave_DeCommoditiesWritable(pais, qtd);

            con.write(chave, valor);

        }
    }

    public static class CombineEtapaB extends Reducer<Text, MaiorPrecoMedio_Chave_DeCommoditiesWritable, Text, MaiorPrecoMedio_Chave_DeCommoditiesWritable> {
        public void reduce(Text key, Iterable<MaiorPrecoMedio_Chave_DeCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String pais = "";

            // verificando qual país possui o maior valor
            // salvando o nome e o valor que cada país apresentou
            for (MaiorPrecoMedio_Chave_DeCommoditiesWritable o : values) {
                if (o.getQtd() > largest) {
                    largest = o.getQtd();
                    pais = o.getPais();
                }
            }

            // chave e valor
            Text chave = new Text(key);
            MaiorPrecoMedio_Chave_DeCommoditiesWritable valores = new MaiorPrecoMedio_Chave_DeCommoditiesWritable(pais, largest);

            con.write(chave, valores);
        }
    }


    public static class ReduceB extends Reducer<Text, MaiorPrecoMedio_Chave_DeCommoditiesWritable, Text, MaiorPrecoMedio_Chave_DeCommoditiesWritable> {
        public void reduce(Text key, Iterable<MaiorPrecoMedio_Chave_DeCommoditiesWritable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String pais = "";

            // verificando qual país possui o maior valor
            // salvando o nome e o valor que cada país apresentou
            for (MaiorPrecoMedio_Chave_DeCommoditiesWritable o : values) {
                if (o.getQtd() > largest) {
                    largest = o.getQtd();
                    pais = o.getPais();
                }
            }

            // chave e valor
            Text chave = new Text(key);
            MaiorPrecoMedio_Chave_DeCommoditiesWritable valores = new MaiorPrecoMedio_Chave_DeCommoditiesWritable(pais, largest);

            con.write(chave, valores);
        }
    }
}
package RECUPERAÇÃO.Q1;

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

public class TransacoesPaisFluxo {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "fluxo_ano");

        //Definição das classes do map, reduce e combine.

        j.setJarByClass(TransacoesPaisFluxo.class); // classe que contem o método MAIN
        j.setMapperClass(Map.class); // classe que contem o método MAP

        j.setReducerClass(Reduce.class); // classe que contem o método REDUCE
        j.setCombinerClass(Combine.class); // classe que contem o método COMBINE

        // Definir os tipos de saída

        j.setMapOutputKeyClass(TransacoesPaisFluxoWritable.class); // tipo de chave de saída do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saída do MAP

        j.setOutputKeyClass(TransacoesPaisFluxoWritable.class); // tipo de chave de saída do REDUCE
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saída do REDUCE

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // Executando a rotina
        j.waitForCompletion(false);
    }

    public static class Map extends Mapper<LongWritable, Text, TransacoesPaisFluxoWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // Covertendo a linha de entrada em uma string
            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                // quebrando em colunas
                String colunas[] = linha.split(";");

                // 2 chaves flow e nome
                String flow = colunas[4];
                String nome = colunas[0];

                TransacoesPaisFluxoWritable keys = new TransacoesPaisFluxoWritable(flow, nome);
                IntWritable valor = new IntWritable(1);// valor unico com um intwrialbe valor 1

                con.write(keys, valor);
            }

        }
    }

    public static class Combine extends Reducer<TransacoesPaisFluxoWritable, IntWritable, TransacoesPaisFluxoWritable, IntWritable> {

        private int count = 0;

        public void reduce(TransacoesPaisFluxoWritable word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // Inicializa a variável cont com o valor zero, que será  ultilizada para acumular a soma dos valores
            int cont = 0;

            // Realiza a contagem dos valores dos IntWritable recebidos
            for (IntWritable v : values) {
                cont += v.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(word, new IntWritable(cont));
                count++;
            }
        }
    }

    public static class Reduce extends Reducer<TransacoesPaisFluxoWritable, IntWritable, TransacoesPaisFluxoWritable, IntWritable> {
        private int count = 0;

        public void reduce(TransacoesPaisFluxoWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            // Inicializa a variável soma com o valor zero, que será  ultilizada para acumular a soma dos valores
            int soma = 0;

            // Soma os valores dos IntWritable recebidos
            for (IntWritable i : values){
                soma += i.get();
            }

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                con.write(key, new IntWritable(soma));
                count++;
            }
        }
    }
}
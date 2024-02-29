package RECUPERAÇÃO.Q2;

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

public class ValorMedioCategoria {

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
        Job job = new Job(c, "valor_categoria");

        // Registro de Classes
        job.setJarByClass(ValorMedioCategoria.class); // classe que contem o método MAIN
        job.setMapperClass(Map.class); // classe que contem o método MAP
        job.setCombinerClass(Combine.class); // classe que contem o método REDUCE
        job.setReducerClass(Reduce.class); // classe que contem o método REDUCE


        // Definir os tipos de saída
        job.setMapOutputKeyClass(Text.class); // tipo de chave de saída do MAP
        job.setMapOutputValueClass(ValorMedioCategoriaWritable.class); // tipo do valor de saída do MAP

        job.setOutputKeyClass(Text.class); // tipo de chave de saída do REDUCE
        job.setOutputValueClass(DoubleWritable.class); // tipo de valor de saída do REDUCE

        // Definindo arquivos de entrada e saída
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Executando a rotina
        job.waitForCompletion(false);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, ValorMedioCategoriaWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Convertendo a linha de entrada em uma string
            String line = value.toString();

            // Ignorando o cabeçalho
            if (!line.startsWith("country_or_area;")) {

                // Quebrando em colunas
                String[] columns = line.split(";");

                // 1 chave (categoria)
                String category = columns[9]; // coluna 9 do csv para categoria

                // 2 valores (preço e quantidade)
                double price = Double.parseDouble(columns[5]);// coluna 5 do csv para preço
                int quantity = 1;

                // Chave, valor
                context.write(new Text(category), new ValorMedioCategoriaWritable(price, quantity));
            }
        }
    }

    public static class Combine extends Reducer<Text, ValorMedioCategoriaWritable, Text, ValorMedioCategoriaWritable> {

        public void reduce(Text key, Iterable<ValorMedioCategoriaWritable> values, Context con)
                throws IOException, InterruptedException {

            // somar as Vals e as qtds para cada chave
            double somaVals = 0;
            int somaQtds = 0;

            for (ValorMedioCategoriaWritable o : values) {
                somaVals += o.getPrice();
                somaQtds += o.getQtd();
            }
            // passando para o reduce valores pre-somados
            con.write(key, new ValorMedioCategoriaWritable(somaVals, somaQtds));

        }
    }

    public static class Reduce extends Reducer<Text, ValorMedioCategoriaWritable, Text, DoubleWritable> {

        private int count;

        public void reduce(Text key, Iterable<ValorMedioCategoriaWritable> values, Context context)
                throws IOException, InterruptedException {

            double totalSum = 0.0;
            int totalCount = 0;

            // Somando os preços e quantidades por categoria
            for (ValorMedioCategoriaWritable value : values) {
                totalSum += value.getPrice();
                totalCount += value.getQtd();
            }

            // Calculando o valor médio
            double average = totalSum / totalCount;

            //context.write(key, new DoubleWritable(average));

            // para manter 5 linhas apenas no output
            if (count <= 4) {
                context.write(key, new DoubleWritable(average));
                count++;
            }
        }
    }
}


package RECUPERAÇÃO.Q3;

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


public class CommoditiesComercializadas {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output.rec/ex3.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        // Criando o primeiro job
        Job j1 = new Job(c, "commodity_1");
        j1.setJarByClass(CommoditiesComercializadas.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);
        j1.setCombinerClass(CombineEtapaA.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // Rodo o job 1
        j1.waitForCompletion(false);

        // Configuracao do job 2
        Job j2 = new Job(c, "commodity_2");
        j2.setJarByClass(CommoditiesComercializadas.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);
        j2.setCombinerClass(CombineEtapaB.class);
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(CommoditiesComercializadasValorWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(CommoditiesComercializadasValorWritable.class);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // Rodo o job 2
        j2.waitForCompletion(false);
    }

    // ETAPA TMP
    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // ignorando o cabeçalho
            if (!linha.startsWith("country_or_area;")) {

                String colunas[] = linha.split(";");

                //pra fazer o id de baixo
                String ano = colunas[1];
                String fluxo = colunas[4];

                // checando se o ano é 2    e o fluxo é importação
                if (ano.equals("2018") && fluxo.equals("Import")) {

                    // chave(Commodity)
                    String commodity = colunas[3];//coluna 3 do csv para comodity
                    // valor(amount)
                    double amount = Double.parseDouble(colunas[8]);// coluna 8 do csv para amount

                    Text chave = new Text(commodity);
                    DoubleWritable valor = new DoubleWritable(amount);

                    con.write(chave, valor);
                }
            }
        }
    }

    public static class CombineEtapaA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            // Inicializa a variável "sum" com o valor 0.0, que será usada para acumular a soma dos valores decimais.
            double sum = 0.0;

            // somando a quantidade
            for (DoubleWritable d : values) {
                sum += d.get();
            }

            // escrevendo o arquivo de resultados
            con.write(key, new DoubleWritable(sum));
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            // Inicializa a variável "sum" com o valor 0.0, que será usada para acumular a soma dos valores decimais.
            double sum = 0.0;

            // somando a quantidade
            for (DoubleWritable d : values) {
                sum += d.get();
            }

            // mandando os resuyltados para o arquivo de saida
            con.write(key, new DoubleWritable(sum));
        }
    }

    //ETAPA TXT
    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, CommoditiesComercializadasValorWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // quebrando a linha por tabs
            String linhas[] = linha.split("\t");

            // valor
            String commodity = linhas[0];
            double qtd = Double.parseDouble(linhas[1]);

            //text sera a chave

            // escrevendo a chave e o valor no contexto
            Text chave = new Text("maior: ");
            CommoditiesComercializadasValorWritable valores = new CommoditiesComercializadasValorWritable(commodity, qtd);// cria o writble com os seus atributos

            con.write(chave, valores);
        }
    }

    public static class CombineEtapaB extends Reducer<Text, CommoditiesComercializadasValorWritable, Text, CommoditiesComercializadasValorWritable> {
        public void reduce(Text key, Iterable<CommoditiesComercializadasValorWritable> values, Context con)
                throws IOException, InterruptedException {

            // Variável "largest" armazena o maior valor encontrado inicialmente como 0.0.
            double largest = 0.0;

            // Variável "commodity" armazena o nome da commodity com o maior valor inicialmente como uma string vazia.
            String commodity = "";

            // verificando qual commodity tem o maior valor
            // salvando o nome e o valor de cada commodity

            for (CommoditiesComercializadasValorWritable c : values) {
                if (c.getQtd() > largest) {
                    largest = c.getQtd();
                    commodity = c.getComm();
                }
            }
            CommoditiesComercializadasValorWritable valores = new CommoditiesComercializadasValorWritable(commodity, largest);

            con.write(key, valores);
        }
    }

    public static class ReduceEtapaB extends Reducer<Text, CommoditiesComercializadasValorWritable, Text, CommoditiesComercializadasValorWritable> {
        public void reduce(Text key, Iterable<CommoditiesComercializadasValorWritable> values, Context con)
                throws IOException, InterruptedException {

            double largest = 0.0;
            String commodity = "";

            // verificando qual commodity tem o maior valor
            // salvando o nome e o valor de cada commodity
            for (CommoditiesComercializadasValorWritable c : values) {
                if (c.getQtd() > largest) {
                    largest = c.getQtd();
                    commodity = c.getComm();
                }
            }

            CommoditiesComercializadasValorWritable valores = new CommoditiesComercializadasValorWritable(commodity, largest);

            con.write(key, valores);
        }
    }
}
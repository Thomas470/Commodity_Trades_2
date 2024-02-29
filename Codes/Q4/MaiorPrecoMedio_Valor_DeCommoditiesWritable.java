package RECUPERAÇÃO.Q4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaiorPrecoMedio_Valor_DeCommoditiesWritable implements WritableComparable<MaiorPrecoMedio_Valor_DeCommoditiesWritable> {

    private double somaValores;
    private int qtd;

    public MaiorPrecoMedio_Valor_DeCommoditiesWritable() {

    }

    public MaiorPrecoMedio_Valor_DeCommoditiesWritable(double somaValores, int qtd) {
        this.somaValores = somaValores;
        this.qtd = qtd;
    }

    public double getSomaValores() {
        return somaValores;
    }

    public void setSomaValores(double somaValores) {
        this.somaValores = somaValores;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(MaiorPrecoMedio_Valor_DeCommoditiesWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(somaValores);
        dataOutput.writeInt(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaValores = dataInput.readDouble();
        qtd = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaiorPrecoMedio_Valor_DeCommoditiesWritable that = (MaiorPrecoMedio_Valor_DeCommoditiesWritable) o;
        return Double.compare(that.somaValores, somaValores) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaValores, qtd);
    }
}
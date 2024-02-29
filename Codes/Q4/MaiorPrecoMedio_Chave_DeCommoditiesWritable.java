package RECUPERAÇÃO.Q4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class MaiorPrecoMedio_Chave_DeCommoditiesWritable implements WritableComparable<MaiorPrecoMedio_Chave_DeCommoditiesWritable> {

    private String pais;
    private double qtd;

    public MaiorPrecoMedio_Chave_DeCommoditiesWritable() {

    }

    public MaiorPrecoMedio_Chave_DeCommoditiesWritable(String pais, double qtd) {
        this.pais = pais;
        this.qtd = qtd;
    }

    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public double getQtd() {
        return qtd;
    }

    public void setQtd(double qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(MaiorPrecoMedio_Chave_DeCommoditiesWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pais);
        dataOutput.writeDouble(qtd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pais = dataInput.readUTF();
        qtd = dataInput.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaiorPrecoMedio_Chave_DeCommoditiesWritable that = (MaiorPrecoMedio_Chave_DeCommoditiesWritable) o;
        return Double.compare(that.qtd, qtd) == 0 && Objects.equals(pais, that.pais);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pais, qtd);
    }

    @Override
    public String toString() {
        return pais + " - " + qtd + " - ";
    }
}
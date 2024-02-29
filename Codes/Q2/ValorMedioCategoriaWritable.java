package RECUPERAÇÃO.Q2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


public class ValorMedioCategoriaWritable implements WritableComparable<ValorMedioCategoriaWritable> {

    private double price;
    private int qtd;

    public ValorMedioCategoriaWritable() {
    }

    public ValorMedioCategoriaWritable(double price, int qtd) {
        this.price = price;
        this.qtd = qtd;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getQtd() {
        return qtd;
    }

    public void setQtd(int qtd) {
        this.qtd = qtd;
    }

    @Override
    public int compareTo(ValorMedioCategoriaWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(price);
        dataOutput.writeInt(qtd);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        price = dataInput.readDouble();
        qtd = dataInput.readInt();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValorMedioCategoriaWritable that = (ValorMedioCategoriaWritable) o;
        return Double.compare(that.price, price) == 0 && qtd == that.qtd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(price, qtd);
    }
}
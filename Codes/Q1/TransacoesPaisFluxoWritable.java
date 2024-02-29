package RECUPERAÇÃO.Q1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TransacoesPaisFluxoWritable implements WritableComparable<TransacoesPaisFluxoWritable> {

    private String pais;
    private String flow;

    public TransacoesPaisFluxoWritable() {

    }

    // Construtor
    public TransacoesPaisFluxoWritable(String pais, String flow) {
        this.pais = pais;
        this.flow = flow;
    }

    // Getters e Setters
    public String getPais() {
        return pais;
    }

    public void setPais(String pais) {
        this.pais = pais;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransacoesPaisFluxoWritable that = (TransacoesPaisFluxoWritable) o;
        return Objects.equals(flow, that.flow) && Objects.equals(pais, that.pais);
    }

    // HashCode e CompareTo
    @Override
    public int hashCode() {
        return Objects.hash(pais, flow);
    }

    @Override
    public int compareTo(TransacoesPaisFluxoWritable o) {
        return Integer.compare(o.hashCode(), this.hashCode());
    }

    // Serialização e Deserialização dos objetos para o envio entre o map reduce e combine

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pais);
        dataOutput.writeUTF(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pais = dataInput.readUTF();
        flow = dataInput.readUTF();
    }

    //retorna uma representação em string da chave

    @Override
    public String toString() {
        return pais + " - " + flow + " -";
    }
}
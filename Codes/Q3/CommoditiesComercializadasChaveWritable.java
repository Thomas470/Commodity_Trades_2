package RECUPERAÇÃO.Q3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommoditiesComercializadasChaveWritable implements WritableComparable<CommoditiesComercializadasChaveWritable> {

    private String flow;
    private String comm;

    public CommoditiesComercializadasChaveWritable() {

    }

    public CommoditiesComercializadasChaveWritable(String flow, String comm) {
        this.flow = flow;
        this.comm = comm;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getComm() {
        return comm;
    }

    public void setComm(String comm) {
        this.comm = comm;
    }

    @Override
    public int compareTo(CommoditiesComercializadasChaveWritable o) {
        int flowComparison = flow.compareTo(o.flow);
        if (flowComparison != 0) {
            return flowComparison;
        }
        return comm.compareTo(o.comm);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flow);
        dataOutput.writeUTF(comm);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flow = dataInput.readUTF();
        comm = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommoditiesComercializadasChaveWritable that = (CommoditiesComercializadasChaveWritable) o;
        return Objects.equals(flow, that.flow) && Objects.equals(comm, that.comm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(flow, comm);
    }

    @Override
    public String toString() {
        return flow + '\t' + comm;
    }
}

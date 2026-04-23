package pjbl.exe7;

import org.apache.hadoop.io.Writable;
import java.io.*;

public class ExportWritable implements Writable {

    private double soma;
    private int count;

    public ExportWritable() {}

    public ExportWritable(double soma, int count) {
        this.soma = soma;
        this.count = count;
    }

    public double getSoma() { return soma; }
    public int getCount() { return count; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        count = in.readInt();
    }
}
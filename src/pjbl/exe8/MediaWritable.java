package pjbl.exe8;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MediaWritable implements Writable {

    private double soma;
    private int count;

    public MediaWritable() {}

    public MediaWritable(double soma, int count) {
        this.soma = soma;
        this.count = count;
    }

    public double getSoma() {
        return soma;
    }

    public int getCount() {
        return count;
    }

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
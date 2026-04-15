package pjbl.exe5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MediaWritable implements Writable {

    private double soma;
    private int count;

    // construtor vazio (OBRIGATÓRIO)
    public MediaWritable() {}

    public MediaWritable(double soma, int count) {
        this.soma = soma;
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeInt(count);
    }

    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        count = in.readInt();
    }

    public double getSoma() {
        return soma;
    }

    public int getCount() {
        return count;
    }

}
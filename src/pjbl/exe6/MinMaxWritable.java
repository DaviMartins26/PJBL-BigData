package pjbl.exe6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxWritable implements Writable {

    private double min;
    private double max;

    public MinMaxWritable() {}

    public MinMaxWritable(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() { return min; }
    public double getMax() { return max; }

    @Override
    public void write(DataOutput out) throws IOException {
        //transforma o dobule em bits e envia do habbot
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //transforma os bits em double
        min = in.readDouble();
        max = in.readDouble();
    }
}
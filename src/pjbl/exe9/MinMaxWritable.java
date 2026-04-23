package pjbl.exe9;

import org.apache.hadoop.io.Writable;

import java.io.*;

//Writable normal
public class MinMaxWritable implements Writable {

    private double min;
    private double max;

    public MinMaxWritable() {}

    public MinMaxWritable(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
    }
}
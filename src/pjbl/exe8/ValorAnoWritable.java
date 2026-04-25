package pjbl.exe8;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ValorAnoWritable implements WritableComparable<ValorAnoWritable> {

    private double valor;
    private String ano;

    public ValorAnoWritable() {}

    public ValorAnoWritable(double valor, String ano) {
        this.valor = valor;
        this.ano = ano;
    }

    public double getValor() {
        return valor;
    }

    public void setValor(double valor) {
        this.valor = valor;
    }

    public String getAno() {
        return ano;
    }

    public void setAno(String ano) {
        this.ano = ano;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(valor);
        out.writeUTF(ano);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valor = in.readDouble();
        ano = in.readUTF();
    }

    // 🔥 AQUI A MÁGICA ACONTECE
    @Override
    public int compareTo(ValorAnoWritable o) {

        // ORDEM DECRESCENTE
        if (this.valor < o.valor) return 1;
        if (this.valor > o.valor) return -1;

        // desempate por ano (opcional, mas bom)
        return this.ano.compareTo(o.ano);
    }
}
package pjbl.exe9;

import org.apache.hadoop.io.WritableComparable;

import java.io.*;

// essa clase vai reestruturar o objeto (Comparable writable) já que não
//podemos fazer contatenção (country + "-"+ year) de strings para chaves ou algo parecido
public class CountryYearWritable implements WritableComparable<CountryYearWritable> {

    private String country;
    private int year;

    public CountryYearWritable() {}

    public CountryYearWritable(String country, int year) {
        this.country = country;
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    // ================= Serilizacao ===========
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeInt(year);
    }

    @Override
    // "reconstroi" o objeto
    public void readFields(DataInput in) throws IOException {
        country = in.readUTF();
        year = in.readInt();
    }

    // ============ ORDENCACAO ==================
    // define a ordem das chaves
    @Override
    public int compareTo(CountryYearWritable o) {
        //primeiro compara por pais
        int cmp = this.country.compareTo(o.country);
        //se for diferente já retorna
        if (cmp != 0) return cmp;
        //Se for o mesmo comprara por ano
        return Integer.compare(this.year, o.year);
    }

    //================ DISTRIBUICAO===============
    //Usada pelo Hadoop para decidir qual reducer recebe a chave
    @Override
    public int hashCode() {
        return country.hashCode() * 163 + year;
    }

    //==========IGUALDADE ===================
    //Define quando duas chaves são consideradas iguais
    @Override
    public boolean equals(Object o) {
        if (o instanceof CountryYearWritable) {
            CountryYearWritable other = (CountryYearWritable) o;
            return country.equals(other.country) && year == other.year;
        }
        return false;
    }

    // ==========Saida-===============
    //define como a chave será impressa no output final (txt)
    @Override
    public String toString() {
        return country + "\t" + year;
    }
}
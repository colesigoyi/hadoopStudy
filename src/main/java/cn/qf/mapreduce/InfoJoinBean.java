package cn.qf.mapreduce;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ program: hadoopStudy
 * @ author:  TaoXueFeng
 * @ create: 2019-09-05 20:05
 * @ desc:
 **/

public class InfoJoinBean implements Writable {
    private int oid;
    private String date;
    private String pid;
    private int amount;
    private String pname;
    private int category_id;
    private double price;

    /**
     * 0表示order，1表示product
     */
    private String flag;

    public InfoJoinBean() {
    }

    public void set(int oid, String date, String pid,
                        int amount, String pname, int category_id, double price, String flag) {
        this.oid = oid;
        this.date = date;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
        this.category_id = category_id;
        this.price = price;
        this.flag = flag;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    String getPname() {
        return pname;
    }

    void setPname(String pname) {
        this.pname = pname;
    }

    int getCategory_id() {
        return category_id;
    }

    void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    double getPrice() {
        return price;
    }

    void setPrice(double price) {
        this.price = price;
    }

    String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "查询信息{" +
                "oid=" + oid +
                ", date='" + date + '\'' +
                ", pid='" + pid + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                ", category_id=" + category_id +
                ", price=" + price +
                '}';
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(oid);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeInt(category_id);
        dataOutput.writeDouble(price);
        dataOutput.writeUTF(flag);
    }
    public void readFields(DataInput dataInput) throws IOException {
        this.oid = dataInput.readInt();
        this.date = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.category_id = dataInput.readInt();
        this.price = dataInput.readDouble();
        this.flag = dataInput.readUTF();
    }
}

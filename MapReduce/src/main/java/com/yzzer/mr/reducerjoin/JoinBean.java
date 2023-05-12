package com.yzzer.mr.reducerjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 实现订单表连接的Bean类， 封装外键，来源表的信息
 */

public class JoinBean implements Writable {
    // 外键
    private String pid;

    // order 表
    private String orderId;
    private Integer mount;

    // pd 表
    private String pname;

    private String origin;

    public JoinBean() {
        super();
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Integer getMount() {
        return mount;
    }

    public void setMount(Integer mount) {
        this.mount = mount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
        return getOrderId() + "\t" + getPname() + "\t" + getMount();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(getOrigin());
        out.writeUTF(getPname());
        out.writeUTF(getOrderId());
        out.writeUTF(getPid());
        out.writeInt(getMount());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setOrigin(in.readUTF());
        setPname(in.readUTF());
        setOrderId(in.readUTF());
        setPid(in.readUTF());
        setMount(in.readInt());
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }
}

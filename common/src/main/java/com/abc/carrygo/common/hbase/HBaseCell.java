package com.abc.carrygo.common.hbase;

public class HBaseCell {
    private String rowKey;
    private String colf;
    private String col;
    private String value;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getColf() {
        return colf;
    }

    public void setColf(String colf) {
        this.colf = colf;
    }

    public String getCol() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}

package com.winter.sqlparser.lineage;

public class ColumnMap {
    private String column;
    private String alias;
    private String func;

    public String getColumn() {
        return column;
    }

    public String getAlias() {
        return alias;
    }

    public String getFunc() {
        return func;
    }

    public ColumnMap(String column, String alias, String func) {
        this.column = column;
        this.alias = alias;
        this.func = func;
    }

    @Override
    public String toString() {
        return "ColumnMap{" +
                "column='" + column + '\'' +
                ", alias='" + alias + '\'' +
                ", func='" + func + '\'' +
                '}';
    }

    public void setColumn(String column) {
        this.column = column;
    }
}

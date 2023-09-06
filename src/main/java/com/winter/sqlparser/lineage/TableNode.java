package com.winter.sqlparser.lineage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableNode {

    private int id;
    private String name;
    private Set<List<String>> lineageName = Sets.newLinkedHashSet();

    //    private List<TableNode> lineage = Lists.newLinkedList();
    //    private List<String> column;
    private TableType type;

    private List<String> groupBy = Lists.newLinkedList();

    private String filter = "";
    private LinkedList<ColumnMap> columnsMaps = Lists.newLinkedList();

    private List<String> columnsInWhere = Lists.newLinkedList();

    private static Map<Integer, TableNode> obj = Maps.newHashMap();

    public static TableNode by(int id, String name, TableType type, List<String> groupBy, String filter,
            LinkedList<ColumnMap> columnMaps, List<String> columnsInWhere) {
        if (obj.containsKey(id)) {
            return obj.get(id);
        } else {
            TableNode res = new TableNode(id, name, type);
            obj.put(id, res);
            return res;
        }
    }


    private TableNode(int id, String name, TableType type) {
        this.id = id;
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public TableType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableNode tableNode = (TableNode) o;

        return new EqualsBuilder().append(id, tableNode.id).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(id).toHashCode();
    }


    @Override
    public String toString() {
        return "TableNode{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", groupBy=" + groupBy +
                ", filter=" + filter +
                '}';
    }

    public Set<List<String>> getLineageName() {
        return lineageName;
    }

    public void setLineageName(Set<List<String>> lineageName) {
        this.lineageName = lineageName;
    }

    public int getId() {
        return id;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public LinkedList<ColumnMap> getColumnsMaps() {
        return columnsMaps;
    }

    public void setColumnsMaps(LinkedList<ColumnMap> columnsMaps) {
        this.columnsMaps = columnsMaps;
    }

    public List<String> getColumnsInWhere() {
        return columnsInWhere;
    }

    public void setColumnsInWhere(List<String> columnsInWhere) {
        this.columnsInWhere = columnsInWhere;
    }
}

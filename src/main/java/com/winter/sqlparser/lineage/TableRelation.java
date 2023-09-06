package com.winter.sqlparser.lineage;

import static com.winter.sqlparser.Constant.HIVE_PREFIX;
import static com.winter.sqlparser.Constant.HIVE_VIEW_PREFIX;
import static com.winter.sqlparser.Constant.LAST_QUERY;
import static com.winter.sqlparser.Constant.tableColumnSplit;
import static com.winter.sqlparser.lineage.TableRelationType.PARENT_CHILD;
import static com.winter.sqlparser.lineage.TableType.SOURCE;
import static com.winter.sqlparser.lineage.TableType.TARGET;
import static com.winter.sqlparser.lineage.TableType.TEMP;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

import jodd.util.StringUtil;

public class TableRelation {
    private int table1id;
    private String table1;
    private TableType table1Type;
    private List<List<String>> table1Lineage;
    private int table2id;
    private String table2;
    private TableType table2Type;
    private List<List<String>> table2Lineage;
    private List<String> groupBy;
    private String filter;
    private List<String> columnsInFilter;
    private String relation;
    private String joinOn;
    private List<ColumnMap> mapList; // column renamed-column function

    private List<String> columnsInSelect;

    public TableRelation(int table1id, String table1, TableType table1Type, int table2id, String table2,
            TableType table2Type,
            List<String> groupBy, String filter, List<String> columnsInFilter, List<ColumnMap> mapList) {
        this.table1id = table1id;
        this.table1 = table1;
        this.table1Type = table1Type;
        this.table2id = table2id;
        this.table2 = table2;
        this.table2Type = table2Type;
        this.filter = filter;
        this.columnsInFilter = CollectionUtils.emptyIfNull(columnsInFilter).stream()
                .filter(Objects::nonNull).filter(StringUtil::isNotEmpty).map(x -> {
                    if (x.contains(tableColumnSplit)) {
                        String[] split = x.split("\\.");
                        if (split.length == 2) {
                            return split[1].toLowerCase(Locale.ROOT);
                        } else if (split.length == 3) {
                            return (split[1] + "." + split[2]).toLowerCase(Locale.ROOT);
                        }
                    }
                    return x.toLowerCase(Locale.ROOT);
                }).collect(Collectors.toList());
        this.relation = PARENT_CHILD.toString();
        this.groupBy = CollectionUtils.emptyIfNull(groupBy).stream()
                .filter(Objects::nonNull).filter(StringUtil::isNotEmpty).map(x -> {
                    if (x.contains(tableColumnSplit)) {
                        String[] split = x.split("\\.");
                        if (split.length == 2) {
                            return split[1].toLowerCase(Locale.ROOT);
                        } else if (split.length == 3) {
                            return (split[1] + "." + split[2]).toLowerCase(Locale.ROOT);
                        }
                    }
                    return x.toLowerCase(Locale.ROOT);
                }).collect(Collectors.toList());
        this.mapList = CollectionUtils.emptyIfNull(mapList)
                .stream().filter(Objects::nonNull).peek(x -> {
                    if (x.getColumn().contains(tableColumnSplit)) {
                        String[] split = x.getColumn().split("\\.");
                        if (split.length == 2) {
                            x.setColumn(split[1].toLowerCase(Locale.ROOT));
                        } else if (split.length == 3) {
                            x.setColumn((split[1] + "." + split[2]).toLowerCase(Locale.ROOT));
                        }
                    }
                }).collect(Collectors.toList());
        this.columnsInSelect = CollectionUtils.emptyIfNull(this.mapList)
                .stream().map(ColumnMap::getColumn).collect(Collectors.toList());
    }

    public TableRelation(int table1id, String table1, TableType table1Type, int table2id, String table2,
            TableType table2Type,
            TableRelationType relation, String joinOn) {
        this.table1id = table1id;
        this.table1 = table1;
        this.table1Type = table1Type;
        this.table2id = table2id;
        this.table2 = table2;
        this.table2Type = table2Type;
        this.relation = relation.toString();
        this.joinOn = joinOn;
    }

    @Override
    public String toString() {
        return "TableRelation{" +
                "table1id=" + table1id +
                ", table1='" + table1 + '\'' +
                ", table1Type=" + table1Type +
                ", table1Lineage=" + table1Lineage +
                ", table2id=" + table2id +
                ", table2='" + table2 + '\'' +
                ", table2Type=" + table2Type +
                ", table2Lineage=" + table2Lineage +
                ", groupBy='" + groupBy + '\'' +
                ", filter='" + filter + '\'' +
                ", columnsInFilter=" + columnsInFilter +
                ", relation='" + relation + '\'' +
                ", joinOn='" + joinOn + '\'' +
                ", mapList=" + mapList +
                '}';
    }

    public String getRelation() {
        return relation;
    }

    public String getTable1() {
        if (StringUtil.isEmpty(getPrefix(table1, table1Type))) {
            return table1;
        }
        return getPrefix(table1, table1Type) + table1;
    }

    public String getTable2() {
        if (StringUtil.isEmpty(getPrefix(table2, table2Type))) {
            return table2;
        }
        return getPrefix(table2, table2Type) + table2;
    }

    private String getPrefix(String name, TableType type) {
        if (type == TEMP) {
            return HIVE_VIEW_PREFIX;
        } else if (type == SOURCE) {
            return HIVE_PREFIX;
        } else if (type == TARGET) {
            if (name.startsWith(LAST_QUERY)) {
                return HIVE_VIEW_PREFIX;
            } else {
                return HIVE_PREFIX;
            }
        }
        return null;
    }

    public List<String> getGroupBy() {
        if (CollectionUtils.isEmpty(this.groupBy)) {
            return null;
        }
        return this.groupBy;
    }

    public String getFilter() {
        return filter;
    }

    public String getJoinOn() {
        return joinOn;
    }

    public TableType getTable1Type() {
        return table1Type;
    }

    public TableType getTable2Type() {
        return table2Type;
    }

    public List<ColumnMap> getMapList() {
        if (CollectionUtils.isEmpty(this.mapList)) {
            return null;
        }
        return this.mapList;
    }

    public List<String> getColumnsInFilter() {
        if (CollectionUtils.isEmpty(this.columnsInFilter)) {
            return null;
        }
        return this.columnsInFilter;
    }

    public List<List<String>> getTable1Lineage() {
        if (CollectionUtils.isEmpty(table1Lineage)) {
            return null;
        }
        return table1Lineage;
    }

    public List<List<String>> getTable2Lineage() {
        if (CollectionUtils.isEmpty(table2Lineage)) {
            return null;
        }
        return table2Lineage;
    }

    public void setTable1Lineage(List<List<String>> table1Lineage) {
        this.table1Lineage = table1Lineage;
    }

    public void setTable2Lineage(List<List<String>> table2Lineage) {
        this.table2Lineage = table2Lineage;
    }

    public int getTable1id() {
        return table1id;
    }

    public int getTable2id() {
        return table2id;
    }

    public List<String> getColumnsInSelect() {
        if (CollectionUtils.isEmpty(this.columnsInSelect)) {
            return null;
        }
        return columnsInSelect;
    }
}

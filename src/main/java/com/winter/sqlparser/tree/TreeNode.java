package com.winter.sqlparser.tree;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.facebook.presto.sql.tree.JoinCriteria;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.winter.sqlparser.lineage.ColumnMap;
import com.winter.sqlparser.lineage.TableType;

public class TreeNode<T> {

    private static AtomicInteger count = new AtomicInteger(0);
    private int id;
    private T data = null;
    private LinkedList<ColumnMap> columnMaps = Lists.newLinkedList();
    private Map<String, LinkedList<String>> resolvedToColumns = Maps.newLinkedHashMap();
    private String queryName;
    private String queryAlias;
    private TableType type;

    private List<String> groupBy = Lists.newLinkedList();

    private String filter;

    private List<String> whereColumns = Lists.newLinkedList();

    private LinkedList<JoinCriteria> join = Lists.newLinkedList();

    private LinkedList<String> setOperation = Lists.newLinkedList();
    //    private List<TreeNode<T>> children = Lists.newCopyOnWriteArrayList();
    private List<TreeNode<T>> children = Lists.newLinkedList();
    private TreeNode<T> parent = null;

    private TreeNode<T> from = null;

    private boolean isSubQuery;

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public TableType getType() {
        return type;
    }

    public void setType(TableType type) {
        this.type = type;
    }

    public TreeNode<T> getFrom() {
        return from;
    }

    public void setFrom(TreeNode<T> from) {
        this.from = from;
    }

    public TreeNode() {
        this.id = count.getAndIncrement();
    }

    public TreeNode(T data) {
        this.id = count.getAndIncrement();
        this.data = data;
    }

    public void removeChild(TreeNode<T> child) {
        this.children.remove(child);
    }

    public void addChild(TreeNode<T> child) {
        child.setParent(this);
        this.children.add(child);
    }

    public void addChild(T data) {
        TreeNode<T> newChild = new TreeNode<>(data);
        this.addChild(newChild);
    }

    public void addChildren(List<TreeNode<T>> children) {
        this.children.addAll(children);
        for (TreeNode<T> t : children) {
            t.setParent(this);
        }
    }

    public Map<String, LinkedList<String>> getResolvedToColumns() {
        return resolvedToColumns;
    }

    public void setResolvedToColumns(Map<String, LinkedList<String>> resolvedToColumns) {
        this.resolvedToColumns = resolvedToColumns;
    }

    public List<TreeNode<T>> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode<T>> children) {
        this.children = children;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public void setParent(TreeNode<T> parent) {
        this.parent = parent;
    }

    public TreeNode<T> getParent() {
        return parent;
    }

    public LinkedList<JoinCriteria> getJoin() {
        return join;
    }

    public void setJoin(LinkedList<JoinCriteria> join) {
        this.join = join;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public void setQueryAlias(String queryAlias) {
        this.queryAlias = queryAlias;
    }

    public String getQueryAlias() {
        return this.queryAlias;
    }

    public void setSetOperation(LinkedList<String> setOperation) {
        this.setOperation = setOperation;
    }

    public LinkedList<String> getSetOperation() {
        return setOperation;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public LinkedList<ColumnMap> getColumnMaps() {
        return columnMaps;
    }

    public void setColumnMaps(LinkedList<ColumnMap> columnMaps) {
        this.columnMaps = columnMaps;
    }


    public TreeNode<T> clone(){
        return new TreeNode<>(this.getData());
    }

    public List<String> getWhereColumns() {
        return whereColumns;
    }

    public void setWhereColumns(List<String> whereColumns) {
        this.whereColumns = whereColumns;
    }


    public boolean getIsSubQuery() {
        return isSubQuery;
    }

    public void setIsSubQuery(boolean isSubQuery) {
        this.isSubQuery = isSubQuery;
    }
}

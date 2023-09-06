package com.winter.sqlparser.lineage;

import static com.winter.sqlparser.Constant.sameSingleColumnSplit;
import static com.winter.sqlparser.Constant.tableColumnSplit;
import static com.winter.sqlparser.lineage.TableRelationType.JOIN;
import static com.winter.sqlparser.lineage.TableRelationType.PARENT_CHILD;
import static com.winter.sqlparser.lineage.TableType.SOURCE;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import com.winter.sqlparser.tree.TreeNode;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpression.Operator;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Union;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import jodd.util.StringUtil;

public class Lineage {
    //    private static Neo4jDao app = new Neo4jDao();
    private static final Logger logger = LoggerFactory.getLogger(Lineage.class);
    private TreeNode<Node> root;

    public Lineage(TreeNode<Node> root) {
        this.root = root;
    }

    // <to, from>
    Map<TableNode, List<TableNode>> m = Maps.newLinkedHashMap();

    List<TableRelation> relations = Lists.newLinkedList();

    public List<List<String>> getTableLineage() {
        for (Entry<TableNode, List<TableNode>> e : m.entrySet()) {
            for (TableNode from : e.getValue()) {
                resolveTableMap(e.getKey(), from);
            }
        }
        List<List<String>> lineage = m.keySet().stream().map(TableNode::getLineageName)
                .flatMap(Collection::stream)
                //                .filter(x -> x.size() > 1)
                .distinct().collect(Collectors.toList());
        return lineage;
    }

    private void resolveTableMap(TableNode to, TableNode from) {
        if (from.getType().equals(SOURCE)) {
            //            to.getLineage().add(from);
            LinkedList<String> l = Lists.newLinkedList();
            l.add(from.getName());
            to.getLineageName().add(l);
            return;
        }
        List<TableNode> childrenOfFrom = m.get(from);
        if (CollectionUtils.isEmpty(childrenOfFrom)) {
            return;
        }
        for (TableNode child : childrenOfFrom) {
            if (from.getId() == child.getId()) {
                return; // stack overflow. such as (select * from a where a.id is not null) as a
            }
            resolveTableMap(from, child);
        }
        //        to.getLineage().addAll(from.getLineage());
        List<String> fromLineage = Lists.newLinkedList();
        for (List<String> l : from.getLineageName()) {
            fromLineage.addAll(l);
        }
        to.getLineageName().add(fromLineage);
    }

    public List<TableRelation> getTableRelation() {
        createRelation(this.root);
        for (Entry<TableNode, List<TableNode>> e : m.entrySet()) {
            for (TableNode from : e.getValue()) {
                resolveTableMap(e.getKey(), from);
            }
        }
        Map<Integer, List<List<String>>> lineageMap =
                m.keySet().stream().map(x -> Pair.of(x.getId(), Lists.newLinkedList(x.getLineageName())))
                        .collect(Collectors.toMap(Pair::getKey,
                                Pair::getValue));
        for (TableRelation r : relations) {
            if (r.getRelation().equals(PARENT_CHILD.toString()) || r.getRelation().equals(JOIN.toString())) {
                List<List<String>> t1 = lineageMap.get(r.getTable1id());
                List<List<String>> t2 = lineageMap.get(r.getTable2id());
                r.setTable1Lineage(t1);
                r.setTable2Lineage(t2);
            }
        }
        return relations;
    }

    public void createRelation(TreeNode<Node> node) {
        if (node == null) {
            return;
        }
        if (CollectionUtils.isNotEmpty(node.getChildren())) {
            for (TreeNode<Node> child : node.getChildren()) {
                createRelation(child);
            }
        }
        TableNode toTable = TableNode.by(node.getId(), node.getQueryName(), node.getType(), node.getGroupBy(),
                node.getFilter(), node.getColumnMaps(), node.getWhereColumns());
        if (node.getFrom() != null) {
            if (node.getFrom().getId() == node.getId()) {
                return;// multi same alias in a sql
            }
            createRelation(node.getFrom());
            TreeNode<Node> from = node.getFrom();
            TableNode fromTable = TableNode.by(from.getId(), from.getQueryName(), from.getType(), from.getGroupBy()
                    , from.getFilter(), from.getColumnMaps(), from.getWhereColumns());
            if (from.getData() instanceof Join) {
                resolveJoinRelation(from.getJoin(), from.getChildren());
                for (TreeNode<Node> joinedT : from.getChildren()) {
                    TableNode joinedTable = TableNode.by(joinedT.getId(), joinedT.getQueryName(), joinedT.getType(),
                            joinedT.getGroupBy(), joinedT.getFilter(), joinedT.getColumnMaps(),
                            joinedT.getWhereColumns());
                    if (joinedT.getData() instanceof SetOperation) {
                        resolveSetOperation(joinedT.getChildren(), joinedTable);
                    }
                    if (!m.containsKey(toTable)) {
                        m.put(toTable, Lists.newLinkedList());
                    }
                    m.get(toTable).add(joinedTable);
                    LinkedList<ColumnMap> columnMapsFromJoin = resolveColumnForJoin(node.getColumnMaps(),
                            joinedT.getQueryAlias(), joinedT.getQueryName());
                    List<String> columnsInWhere = resolveColumnForWhere(node.getWhereColumns(),
                            joinedT.getQueryAlias(), joinedT.getQueryName());
                    relations.add(new TableRelation(node.getId(), node.getQueryName(), node.getType(), joinedT.getId(),
                            joinedT.getQueryName(),
                            joinedT.getType(), node.getGroupBy(), node.getFilter(), columnsInWhere,
                            columnMapsFromJoin));
                }
            } else if (from.getData() instanceof SetOperation) {
                if (!(node.getData() instanceof Union)) {
                    resolveSetOperation(from.getFrom().getChildren(), fromTable);
                    if (!m.containsKey(toTable)) {
                        m.put(toTable, Lists.newLinkedList());
                    }
                    m.get(toTable).add(fromTable);
                    relations.add(new TableRelation(node.getId(), node.getQueryName(), node.getType(), from.getId(),
                            from.getQueryName(),
                            from.getType(), node.getGroupBy(), node.getFilter(), node.getWhereColumns(),
                            node.getColumnMaps()));
                }
            } else {
                if (!(node.getData() instanceof Union)) {
                    if (!m.containsKey(toTable)) {
                        m.put(toTable, Lists.newLinkedList());
                    }
                    m.get(toTable).add(fromTable);
                    relations.add(new TableRelation(node.getId(), node.getQueryName(), node.getType(), from.getId(),
                            from.getQueryName(),
                            from.getType(), node.getGroupBy(), node.getFilter(), node.getWhereColumns(),
                            node.getColumnMaps()));
                }
            }
        }
    }

    private List<String> resolveColumnForWhere(List<String> columns, String alias, String name) {
        List<String> res = Lists.newLinkedList();
        for (String c : columns) {
            StringBuilder sb = new StringBuilder();
            List<String> cs = Arrays.stream(c.split(sameSingleColumnSplit))
                    .filter(Objects::nonNull).filter(StringUtils::isNotEmpty)
                    .distinct().collect(Collectors.toList());
            for (String s : cs) {
                if (s.contains(tableColumnSplit)) {
                    String[] split = s.split("\\.");
                    if (split.length == 3) {
                        // schema.table.column
                    } else if (split.length == 2) {
                        if (split[0].equals(alias) || split[0].equals(name)) {
                            sb.append(split[1]).append(sameSingleColumnSplit);
                        }
                    }
                } else {
                    // here is a join, and just write column name, need HMS to identify this column
                    // belongs to which table
                    sb.append(s).append(sameSingleColumnSplit);
                }
            }
            if (!StringUtil.isEmpty(sb.toString())) {
                res.add(sb.deleteCharAt(sb.length() - 1).toString());
            }
        }
        return res;
    }


    private void resolveSetOperation(List<TreeNode<Node>> queries, TableNode parentTable) {
        for (int i = 0; i < queries.size(); i++) {
            TreeNode<Node> set = queries.get(i);
            TableNode childTable = TableNode.by(set.getId(), set.getQueryName(), set.getType(), set.getGroupBy()
                    , set.getFilter(), set.getColumnMaps(), set.getWhereColumns());
            if (!m.containsKey(parentTable)) {
                m.put(parentTable, Lists.newLinkedList());
            }
            m.get(parentTable).add(childTable);
            relations.add(
                    new TableRelation(parentTable.getId(), parentTable.getName(), parentTable.getType(), set.getId(),
                            set.getQueryName(),
                            set.getType(), parentTable.getGroupBy(), parentTable.getFilter(),
                            parentTable.getColumnsInWhere(),
                            parentTable.getColumnsMaps()));
            if (i == queries.size() - 1) {
                continue;
            }
            TreeNode<Node> fromTable = queries.get(i);
            TreeNode<Node> toTable = queries.get(i + 1);
            relations.add(
                    new TableRelation(fromTable.getId(), fromTable.getQueryName(), fromTable.getType(), toTable.getId(),
                            toTable.getQueryName(),
                            toTable.getType(), TableRelationType.UNION, null));

        }
    }

    private void resolveJoinRelation(LinkedList<JoinCriteria> joinOns, List<TreeNode<Node>> joinedTable) {
        // this map <Alias, TreeNode>
        Map<String, TreeNode<Node>> map = Maps.newHashMap();
        for (TreeNode<Node> t : joinedTable) {
            map.put(t.getQueryAlias(), t);
        }
        for (int i = 0; i < joinOns.size(); i++) {
            JoinCriteria j = joinOns.get(i);
            if (j instanceof JoinOn) {
                JoinOn joinOn = (JoinOn) j;
                if (joinOn.getExpression() instanceof ComparisonExpression) {
                    resolveComparisonExpressionJoinOn((ComparisonExpression) joinOn.getExpression(), map, joinedTable,
                            i);
                } else if (joinOn.getExpression() instanceof LogicalBinaryExpression) {
                    Expression left = ((LogicalBinaryExpression) joinOn.getExpression()).getLeft();
                    Expression right = ((LogicalBinaryExpression) joinOn.getExpression()).getRight();
                    if (left instanceof ComparisonExpression) {
                        resolveComparisonExpressionJoinOn((ComparisonExpression) left, map, joinedTable, i);
                    }
                    if (right instanceof ComparisonExpression) {
                        resolveComparisonExpressionJoinOn((ComparisonExpression) right, map, joinedTable, i);
                    }
                }
            } else if (j instanceof JoinUsing) {
                JoinUsing ju = (JoinUsing) j;
                List<String> using = ju.getColumns().stream().map(Identifier::getValue).collect(Collectors.toList());
                TreeNode<Node> lTable = joinedTable.get(i);
                TreeNode<Node> rTable = joinedTable.get(i + 1);
                relations.add(new TableRelation(lTable.getId(), lTable.getQueryName(), lTable.getType(), rTable.getId(),
                        rTable.getQueryName(),
                        rTable.getType(), JOIN, using + "=" + using));
            } else if (j instanceof NaturalJoin) {
                logger.error(String.format("here is a natural join:%s", j));
            }
        }

    }

    private void resolveComparisonExpressionJoinOn(ComparisonExpression ce, Map<String, TreeNode<Node>> map,
            List<TreeNode<Node>> joinedTable, int i) {
        Expression l = ce.getLeft();
        Expression r = ce.getRight();
        Operator operator = ce.getOperator();
        if (l instanceof DereferenceExpression && r instanceof DereferenceExpression) {
            DereferenceExpression ldf = (DereferenceExpression) l;
            DereferenceExpression rdf = (DereferenceExpression) r;
            resolveJoinDereference(ldf, rdf, map, operator);
        } else {
            //                        logger.error(String.format("There's a JoinOn here. It's not a
            //                        DereferenceExpression. %s", j));
            TreeNode<Node> lt = joinedTable.get(i);
            TreeNode<Node> rt = joinedTable.get(i + 1);
            TableRelation joinedR = new TableRelation(lt.getId(), lt.getQueryName(), lt.getType(),
                    rt.getId(), rt.getQueryName(), rt.getType(), JOIN, ce.toString());
            relations.add(joinedR);
        }
    }

    @Deprecated
    private DereferenceExpression getDeferenceOfJoinCriteria(Expression join) {
        if (join instanceof DereferenceExpression) {
            return (DereferenceExpression) join;
        } else if (join instanceof Cast) {
            Cast joinC = (Cast) join;
            return getDeferenceOfJoinCriteria(joinC.getExpression());
        } else if (join instanceof FunctionCall) {
            FunctionCall joinFC = (FunctionCall) join;
            for (Expression argument : joinFC.getArguments()) {
                if (argument instanceof DereferenceExpression) {
                    DereferenceExpression joinFCDE = (DereferenceExpression) argument;
                    return joinFCDE;
                } else {
                    if (getDeferenceOfJoinCriteria(argument) != null) {
                        return getDeferenceOfJoinCriteria(argument);
                    }
                }
            }
            return null;
        } else {
            return null;
        }
    }

    private void resolveJoinDereference(DereferenceExpression l, DereferenceExpression r,
            Map<String, TreeNode<Node>> map, Operator operator) {
        TableNode lTableNode = getTableNodeOfJoinDE(l, map);
        String lField = getBaseAndField(l, map).getValue();
        TableNode rTableNode = getTableNodeOfJoinDE(r, map);
        String rField = getBaseAndField(r, map).getValue();
        if (lTableNode == null || rTableNode == null) {
            return;
        }
        relations.add(new TableRelation(lTableNode.getId(), lTableNode.getName(), lTableNode.getType(),
                rTableNode.getId(), rTableNode.getName(),
                rTableNode.getType(), JOIN, lField + operator.getValue() + rField));
    }

    private TableNode getTableNodeOfJoinDE(DereferenceExpression de, Map<String, TreeNode<Node>> map) {
        TreeNode<Node> table = map.get(getBaseAndField(de, map).getKey());
        if (table == null) {
            //            logger.error("here is a DereferenceExpression but we can't find the source table");
            return null;
        }
        return TableNode.by(table.getId(), table.getQueryName(), table.getType(), table.getGroupBy(), table.getFilter()
                , table.getColumnMaps(), table.getWhereColumns());
    }

    private Pair<String, String> getBaseAndField(DereferenceExpression de, Map<String, TreeNode<Node>> map) {
        List<String> l = Arrays.stream(de.toString().split("\\.")).collect(Collectors.toList());
        for (int i = 1; i < l.size(); i++) {
            String base = String.join(".", l.subList(0, i));
            if (map.containsKey(base.toLowerCase(Locale.ROOT))) {
                return Pair.of(base.toLowerCase(Locale.ROOT), String.join(".", l.subList(i, l.size())));
            } else if (map.containsKey(base.toUpperCase(Locale.ROOT))) {
                return Pair.of(base.toUpperCase(Locale.ROOT), String.join(".", l.subList(i, l.size())));
            }
        }
        return Pair.of("", "");
    }

    private LinkedList<ColumnMap> resolveColumnForJoin(LinkedList<ColumnMap> columnMaps, String fromAlias,
            String queryName) {
        LinkedList<ColumnMap> res = Lists.newLinkedList();
        for (int i = 0; i < columnMaps.size(); i++) {
            String c = columnMaps.get(i).getColumn();
            if (c.contains(sameSingleColumnSplit)) {
                StringBuilder sb = new StringBuilder();
                List<String> cs = Arrays.stream(c.split(sameSingleColumnSplit)).collect(Collectors.toList());
                for (String s : cs) {
                    if (s.contains(tableColumnSplit)) {
                        String[] split = s.split("\\.");
                        if (split.length == 3) {
                            // schema.table.column
                        } else if (split.length == 2) {
                            if (split[0].equals(fromAlias) || split[0].equals(queryName)) {
                                sb.append(split[1]).append(sameSingleColumnSplit);
                            }
                        }
                    } else {
                        // here is a join, and just write column name, need HMS to identify this column
                        // belongs to which table
                        sb.append(s).append(sameSingleColumnSplit);
                    }
                }
                if (!StringUtil.isEmpty(sb.toString())) {
                    res.add(new ColumnMap(sb.deleteCharAt(sb.length() - 1).toString(), columnMaps.get(i).getAlias(),
                            columnMaps.get(i).getFunc()));
                }
            } else {
                if (c.contains(tableColumnSplit)) {
                    String[] split = c.split("\\.");
                    if (split.length == 3) {
                        // schema.table.column
                    } else if (split.length == 2) {
                        if (split[0].equals(fromAlias) || split[0].equals(queryName)) {
                            res.add(new ColumnMap(split[1], columnMaps.get(i).getAlias(), columnMaps.get(i).getFunc()));
                        }
                    }
                } else {
                    // here is a join, and just write column name, need HMS to identify this column
                    // belongs to which table
                    res.add(new ColumnMap(c, columnMaps.get(i).getAlias(), columnMaps.get(i).getFunc()));
                }
            }
        }
        return res;
    }


    public void createNode(TreeNode<Node> node) throws Exception {
        if (node == null) {
            return;
        }
        if (node.getFrom() != null) {
            createNode(node.getFrom());
        }
        if (CollectionUtils.isNotEmpty(node.getChildren())) {
            for (TreeNode<Node> child : node.getChildren()) {
                createNode(child);
            }
        }
        if (node.getData() instanceof QuerySpecification) {
            //            app.createTableIfNotExists(node.getQueryAlias());
            LinkedList<ColumnMap> columnMaps = node.getColumnMaps();
            //            app.createColumnIfNotExist(columns, node.getQueryAlias());
        } else if (node.getData() instanceof Table) {
            //            app.createTableIfNotExists(((Table) node.getData()).getName().toString());
        }
    }

}

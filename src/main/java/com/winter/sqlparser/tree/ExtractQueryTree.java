package com.winter.sqlparser.tree;

import static com.winter.sqlparser.Constant.LAST_QUERY;
import static com.winter.sqlparser.Constant.sameSingleColumnSplit;
import static com.winter.sqlparser.Constant.tableColumnSplit;
import static com.winter.sqlparser.lineage.TableType.SOURCE;
import static com.winter.sqlparser.lineage.TableType.TARGET;
import static com.winter.sqlparser.lineage.TableType.TEMP;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.winter.sqlparser.util.CommonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.winter.sqlparser.lineage.ColumnMap;

public class ExtractQueryTree {
    private static final Logger logger = LoggerFactory.getLogger(ExtractQueryTree.class);
    private Statement statement;
    private AtomicInteger count;

    public ExtractQueryTree(Statement statement) {
        this.statement = statement;
        this.count = new AtomicInteger();
    }

    private Set<String> tmpTable = Sets.newLinkedHashSet();

    public TreeNode<Node> getQueryTree(int i) {
        try {
            TreeNode<Node> query = null;
            if (statement instanceof Query) {
                query = resolveQuery((Query) statement);
            } else if (statement instanceof Insert) {
                query = resolveQuery(((Insert) statement).getQuery());
            } else if (statement instanceof CreateTableAsSelect) {
                query = resolveQuery(((CreateTableAsSelect) statement).getQuery());
            } else {
                //                logger.error(String.format("here is a sql we ignore. sql class:%s", statement
                //                .getClass()));
                return null;
            }
            tmpTable = getTmpTable(query);
            addQueryAndTableName(query);
            pruning(query);
            pruningFrom(query);
            unionFromAndWith(query);
            labelQuery(query);
            query = setLastQueryInfo(query, i);
            return query;
        } catch (Exception spe) {
//            logger.error(String.format("here is a sql parser exception. sql:%s", statement));
            return null;
        }
    }

    private TreeNode<Node> setLastQueryInfo(TreeNode<Node> node, int i) {
        TreeNode<Node> res = node;
        if (CollectionUtils.isNotEmpty(node.getSetOperation())) {
            res = new TreeNode<>(new Table(new NodeLocation(0, 0), QualifiedName.of(LAST_QUERY)));
            res.setFrom(node);
            node.setParent(res);
        }
        if (statement instanceof Query) {
            res.setQueryName(LAST_QUERY + i);
        } else if (statement instanceof Insert) {
            res.setQueryName(((Insert) statement).getTarget().toString());
        } else if (statement instanceof CreateTableAsSelect) {
            res.setQueryName(((CreateTableAsSelect) statement).getName().toString());
        }
        res.setType(TARGET);
        return res;
    }

    private void unionFromAndWith(TreeNode<Node> node) {
        if (node == null) {
            return;
        }
        for (TreeNode<Node> n : CollectionUtils.emptyIfNull(node.getChildren())) {
            unionFromAndWith(n);
        }
        Map<String, Integer> view = Maps.newLinkedHashMap();
        Map<String, TreeNode<Node>> setOperation = Maps.newLinkedHashMap();
        for (TreeNode<Node> n : CollectionUtils.emptyIfNull(node.getChildren())) {
            if (n.getData() instanceof SetOperation) {
                setOperation.put(n.getQueryName(), n);
            }
            String queryName = n.getQueryName();
            unionFromAndWith(n.getFrom(), view, setOperation);
            view.put(queryName, n.getId());
        }
        unionFromAndWith(node.getFrom(), view, setOperation);
    }

    private void unionFromAndWith(TreeNode<Node> from, Map<String, Integer> view,
            Map<String, TreeNode<Node>> setOperation) {
        if (from == null) {
            return;
        }
        unionFromAndWith(from.getFrom(), view, setOperation);
        for (TreeNode<Node> n : CollectionUtils.emptyIfNull(from.getChildren())) {
            unionFromAndWith(n, view, setOperation);
        }
        if (view.containsKey(from.getQueryName()) && !from.getIsSubQuery()) {
            from.setId(view.get(from.getQueryName()));
        }
        if (setOperation.containsKey(from.getQueryName())) {
            from.setId(setOperation.get(from.getQueryName()).getId());
            from.setFrom(setOperation.get(from.getQueryName()));
        }
    }

    private void labelQuery(TreeNode<Node> node) {
        if (node == null) {
            return;
        }
        if (CollectionUtils.isNotEmpty(node.getChildren())) {
            for (TreeNode<Node> child : node.getChildren()) {
                labelQuery(child);
            }
        }
        if (node.getFrom() != null) {
            if (node.getId() == node.getFrom().getId()) {
                node.setType(TEMP);
                return;
            }
            labelQuery(node.getFrom());
        }
        if (tmpTable.contains(node.getQueryName().toLowerCase(Locale.ROOT))) {
            node.setType(TEMP);
        } else {
            node.setType(SOURCE);
        }
    }

    Map<QuerySpecification, String> name = Maps.newLinkedHashMap();
    Map<SetOperation, String> setName = Maps.newLinkedHashMap();
    Map<Join, String> joinName = Maps.newLinkedHashMap();

    private void addQueryAndTableName(TreeNode<Node> node) {
        if (node == null) {
            return;
        }
        addQueryAndTableName(node.getFrom());
        for (TreeNode<Node> child : node.getChildren()) {
            addQueryAndTableName(child);
        }
        if (node.getData() instanceof QuerySpecification) {
            if (StringUtils.isEmpty(node.getQueryName())) {
                if (name.containsKey((QuerySpecification) node.getData())) {
                    node.setQueryName(name.get((QuerySpecification) node.getData()));
                } else {
                    String n = "anonymous" + count.getAndIncrement();
                    node.setQueryName(n);
                    name.put((QuerySpecification) node.getData(), n);
                    tmpTable.add(n.toLowerCase(Locale.ROOT));
                }
            }
        } else if (node.getData() instanceof Table) {
            String n = ((Table) node.getData()).getName().toString();
            node.setQueryName(n);
            node.setQueryAlias(n);
        } else if (node.getData() instanceof SetOperation) {
            if (StringUtils.isEmpty(node.getQueryName())) {
                if (setName.containsKey((SetOperation) node.getData())) {
                    node.setQueryName(setName.get((SetOperation) node.getData()));
                } else {
                    String n = "setOperation" + count.getAndIncrement();
                    node.setQueryName(n);
                    node.getFrom().setQueryName(n);
                    setName.put((SetOperation) node.getData(), n);
                    tmpTable.add(n.toLowerCase(Locale.ROOT));
                }
            }

        } else if (node.getData() instanceof Join) {
            if (StringUtils.isEmpty(node.getQueryName())) {
                if (joinName.containsKey((Join) node.getData())) {
                    node.setQueryName(joinName.get((Join) node.getData()));
                } else {
                    String n = "Join" + count.getAndIncrement();
                    node.setQueryName(n);
                    joinName.put((Join) node.getData(), n);
                    tmpTable.add(n.toLowerCase(Locale.ROOT));
                }
            }
        }
    }

    private Set<String> getTmpTable(TreeNode<Node> node) {
        Set<String> res = Sets.newLinkedHashSet();
        if (CollectionUtils.isNotEmpty(node.getChildren())) {
            for (TreeNode<Node> child : node.getChildren()) {
                res.addAll(getTmpTable(child));
            }
        }
        if (node.getFrom() != null) {
            res.addAll(getTmpTable(node.getFrom()));
        }
        if (node.getData() instanceof Identifier) {
            res.add(((Identifier) node.getData()).getValue().toLowerCase(Locale.ROOT));
        }
        return res;
    }

    private void pruningFrom(TreeNode<Node> node) {
        if (node == null) {
            return;
        }

        for (TreeNode<Node> child : node.getChildren()) {
            pruningFrom(child);
        }

        if (node.getFrom() != null) {
            TreeNode<Node> from = node.getFrom();
            pruningFrom(from);
            if (from.getData() instanceof Identifier) {
                if (from.getChildren().size() != 1) {
                    logger.error(String.format("alias err. %s", from.getChildren()));
                }
                TreeNode<Node> aliasedNode = from.getChildren().get(0);
                if (aliasedNode.getData() instanceof QuerySpecification) {
                    aliasedNode.setQueryName(((Identifier) from.getData()).getValue());
                    pruningFrom(aliasedNode);
                } else if (aliasedNode.getData() instanceof Table) {
                    aliasedNode.setQueryName(((Table) aliasedNode.getData()).getName().toString());
                }
                aliasedNode.setQueryAlias(((Identifier) from.getData()).getValue());
                node.setFrom(aliasedNode);
            } else if (from.getData() instanceof Join) {
                LinkedList<TreeNode<Node>> joined = Lists.newLinkedList(from.getChildren());
                for (TreeNode<Node> j : joined) {
                    pruning(j);
                }
            }
        }
    }


    private void pruning(TreeNode<Node> node) {
        if (node == null) {
            return;
        }

        for (TreeNode<Node> child : node.getChildren()) {
            pruning(child);
        }

        if (node.getData() instanceof Identifier) {
            List<TreeNode<Node>> children = node.getChildren();
            if (children.size() != 1) {
                logger.error(String.format("alias err. %s", children));
            }
            TreeNode<Node> aliasedNode = children.get(0);
            if (aliasedNode.getData() instanceof QuerySpecification) {
                aliasedNode.setQueryName(((Identifier) node.getData()).getValue());
                // subquery contains table
                pruningFrom(aliasedNode);
            } else if (aliasedNode.getData() instanceof Table) {
                aliasedNode.setQueryName(((Table) aliasedNode.getData()).getName().toString());
            } else if (aliasedNode.getData() instanceof SetOperation) {
                //                aliasedNode.setQueryName(((Identifier) node.getData()).getValue());
                aliasedNode.setQueryAlias(aliasedNode.getQueryName());
                String name = node.getData().toString();
                node.setQueryName(name);
                node.setQueryAlias(name);
                node.setData(new Table(new NodeLocation(0, 0), QualifiedName.of(name)));
                node.setFrom(node.getChildren().get(0));
                node.setChildren(Lists.newLinkedList());
                return;
            }
            aliasedNode.setQueryAlias(((Identifier) node.getData()).getValue());
            TreeNode<Node> parent = node.getParent();
            LinkedList<TreeNode<Node>> c = Lists.newLinkedList(parent.getChildren());
            for (int i = 0; i < c.size(); i++) {
                if (c.get(i).equals(node)) {
                    c.set(i, aliasedNode);
                }
            }
            for (TreeNode<Node> n : c) {
                n.setParent(parent);
            }
            parent.setChildren(c);
        }

    }

    private TreeNode<Node> resolveQuery(Query query) {
        LinkedList<TreeNode<Node>> with = null;
        if (query.getWith().isPresent()) {
            with = resolveWith(query.getWith().get());
        }
        QueryBody queryBody = query.getQueryBody();
        TreeNode<Node> queryNode = resolveQueryBody(queryBody);
        if (queryNode != null && CollectionUtils.isNotEmpty(with)) {
            for (TreeNode<Node> w : with) {
                queryNode.addChild(w);
            }
        }
        return queryNode;
    }


    private LinkedList<TreeNode<Node>> resolveWith(With with) {
        List<WithQuery> queries = with.getQueries();
        LinkedList<TreeNode<Node>> res = Lists.newLinkedList();
        for (WithQuery query : queries) {
            TreeNode<Node> withNode = new TreeNode<>(query.getName());
            TreeNode<Node> queryNode = resolveQuery(query.getQuery());
            if (queryNode != null) {
                withNode.addChild(queryNode);
            }
            res.add(withNode);
        }
        return res;
    }


    private TreeNode<Node> resolveQueryBody(QueryBody queryBody) {
        if (queryBody instanceof SetOperation) {
            SetOperation so = (SetOperation) queryBody;
            TreeNode<Node> res = new TreeNode<>(so);
            Pair<LinkedList<TreeNode<Node>>, LinkedList<String>> p = resolveSetOperation(so);
            LinkedList<TreeNode<Node>> children = p.getKey();
            LinkedList<String> setOperation = p.getValue();
            Collections.reverse(setOperation);
            if (CollectionUtils.isNotEmpty(p.getKey())) {
                //                res.addChildren(children);
                res.setSetOperation(setOperation);
                TreeNode<Node> from = new TreeNode<>(new Table(new NodeLocation(0, 0), QualifiedName.of("so")));
                from.setChildren(children);
                res.setFrom(from);
            } else {
                logger.warn(
                        String.format("a SetOperation doesn't contains any valid relation. SetOperation: %s", so));
            }
            return res;
        } else if (queryBody instanceof Table) {
        } else if (queryBody instanceof QuerySpecification) {
            QuerySpecification qs = (QuerySpecification) queryBody;
            TreeNode<Node> res = new TreeNode<>(qs);
            LinkedList<ColumnMap> columnMaps = resolveSelect(qs.getSelect());
            res.setColumnMaps(columnMaps);
            if (qs.getFrom().isPresent()) {
                Relation from = qs.getFrom().get();
                TreeNode<Node> resolvedFrom = resolveRelation(from);
                if (resolvedFrom != null) {
                    res.setFrom(resolvedFrom);
                } else {
                    logger.warn(String.format("a from doesn't contains any valid relation. From: %s ", from));
                }
            }

            if (qs.getGroupBy().isPresent()) {
                List<String> groupByResult = Lists.newLinkedList();
                GroupBy groupBy = qs.getGroupBy().get();
                List<GroupingElement> groupingElements = groupBy.getGroupingElements();
                for (GroupingElement groupingElement : groupingElements) {
                    List<Expression> expressions = groupingElement.getExpressions();
                    for (Expression expression : expressions) {
                        String s = expression.toString();
                        if (CommonUtil.isInteger(s)) {
                            if (Integer.parseInt(s) - 1 < columnMaps.size()) {
                                groupByResult.add(columnMaps.get(Integer.parseInt(s) - 1).getColumn());
                            }
                        } else {
                            groupByResult.add(s);
                        }
                    }
                }
                res.setGroupBy(groupByResult);
            }

            if (qs.getWhere().isPresent()) {
                Expression expression = qs.getWhere().get();
                res.setFilter(expression.toString());
                String whereColumns = getColumn(expression);
                if (StringUtils.isNotEmpty(whereColumns)) {
                    List<String> columnsInWhere =
                            Arrays.stream(whereColumns.split(sameSingleColumnSplit)).filter(Objects::nonNull)
                                    .filter(StringUtils::isNotEmpty)
                                    .distinct().map(x -> {
                                        if (x.contains(tableColumnSplit)) {
                                            String[] split = x.split("\\.");
                                            if (split.length == 2) {
                                                return split[1];
                                            }
                                        }
                                        return x;
                                    }).collect(Collectors.toList());
                    res.setWhereColumns(columnsInWhere);
                }
            }

            return res;
        } else if (queryBody instanceof TableSubquery) {
            TableSubquery ts = (TableSubquery) queryBody;
            return resolveQuery(ts.getQuery());
        } else if (queryBody instanceof Values) {
            Values values = (Values) queryBody;
            TreeNode<Node> v = new TreeNode<>(values);
            v.setQueryName("USER_DEFINED_VALUES");
            v.setQueryAlias("USER_DEFINED_VALUES");
            return v;
        }
        return null;
    }

    private TreeNode<Node> resolveRelation(Relation r) {
        if (r == null) {
            return null;
        }
        if (r instanceof AliasedRelation) {
            AliasedRelation ar = (AliasedRelation) r;
            // this alias 1. table alias 2. subquery alias
            TreeNode<Node> res = new TreeNode<>(ar.getAlias());
            TreeNode<Node> resolvedAR = resolveRelation(ar.getRelation());
            if (resolvedAR.getData() instanceof QuerySpecification) {
                resolvedAR.setIsSubQuery(true);
            }
            if (resolvedAR.getData() instanceof SetOperation) {
                res.setIsSubQuery(true);
            }
            if (resolvedAR != null) {
                res.addChild(resolvedAR);
            } else {
                logger.warn(String.format("a AliasedRelation doesn't contains any valid relation. AliasedRelation: %s",
                        ar));
            }
            return res;
        } else if (r instanceof SetOperation) {
            SetOperation sor = (SetOperation) r;
            return resolveQueryBody(sor);
        } else if (r instanceof Join) {
            Join jr = (Join) r;
            TreeNode<Node> res = new TreeNode<>(jr);
            Pair<LinkedList<TreeNode<Node>>, LinkedList<JoinCriteria>> resolvedJoin = resolveJoin(jr);
            LinkedList<TreeNode<Node>> children = resolvedJoin.getKey();
            LinkedList<JoinCriteria> joinOns = resolvedJoin.getValue();
            Collections.reverse(joinOns);
            res.setJoin(joinOns);
            if (CollectionUtils.isNotEmpty(children)) {
                res.addChildren(children);
            } else {
                logger.warn(String.format("a Join doesn't contains any valid relation. Join: %s", jr));
            }
            return res;
        } else if (r instanceof Lateral) {
            Lateral lr = (Lateral) r;
            TreeNode<Node> res = new TreeNode<>(lr);
            TreeNode<Node> resolvedLR = resolveQuery(lr.getQuery());
            if (resolvedLR != null) {
                res.addChild(resolvedLR);
            } else {
                logger.warn(String.format("a Lateral doesn't contains any valid relation. Lateral: %s", lr));
            }
            return res;
        } else if (r instanceof Table) {
            Table tr = (Table) r;
            return new TreeNode<>(tr);
        } else if (r instanceof QueryBody) {
            QueryBody qbr = (QueryBody) r;
            return resolveQueryBody(qbr);
        } else if (r instanceof SampledRelation) {
            SampledRelation sr = (SampledRelation) r;
            logger.warn(String.format("here is a SampledRelation, need to notice. SampledRelation %s", r));
            TreeNode<Node> res = new TreeNode<>(sr);
            TreeNode<Node> resolvedSR = resolveRelation(sr.getRelation());
            if (resolvedSR != null) {
                res.addChild(resolvedSR);
            } else {
                logger.warn(String.format("a SampledRelation doesn't contains any valid relation. SampledRelation: %s",
                        sr));
            }
            return res;
        } else if (r instanceof Unnest) {
            Unnest ur = (Unnest) r;
            TreeNode<Node> res = new TreeNode<>(ur);
            String unnestOf = ur.getExpressions().stream().map(Expression::toString).collect(Collectors.joining(","));
            res.setQueryName(String.format("UNNEST:%s", unnestOf));
            res.setQueryAlias(String.format("UNNEST:%s", unnestOf));
            return res;
        }
        return null;
    }

    private Pair<LinkedList<TreeNode<Node>>, LinkedList<String>> resolveSetOperation(SetOperation so) {
        LinkedList<TreeNode<Node>> res = Lists.newLinkedList();
        LinkedList<String> operation = Lists.newLinkedList();
        if (so instanceof Union) {
            operation.add("UNION");
        } else if (so instanceof Except) {
            operation.add("EXCEPT");
        } else if (so instanceof Intersect) {
            operation.add("INTERSECT");
        }
        for (Relation sor : so.getRelations()) {
            if (sor instanceof SetOperation) {
                Pair<LinkedList<TreeNode<Node>>, LinkedList<String>> p = resolveSetOperation((SetOperation) sor);
                res.addAll(p.getKey());
                operation.addAll(p.getValue());
            } else {
                TreeNode<Node> resolvedSOR = resolveRelation(sor);
                if (resolvedSOR != null) {
                    res.add(resolvedSOR);
                } else {
                    logger.warn(
                            String.format("a SetOperation doesn't contains any valid relation. SetOperation: %s", so));
                }
            }

        }
        return Pair.of(res, operation);
    }

    private Pair<LinkedList<TreeNode<Node>>, LinkedList<JoinCriteria>> resolveJoin(Join join) {
        LinkedList<TreeNode<Node>> res = Lists.newLinkedList();
        LinkedList<JoinCriteria> joinOns = Lists.newLinkedList();
        if (join == null) {
            return Pair.of(res, joinOns);
        }
        if (join.getCriteria().isPresent()) {
            joinOns.add(join.getCriteria().get());
        }
        Relation leftJoin = join.getLeft();
        if (leftJoin instanceof Join) {
            Join lj = (Join) leftJoin;
            Pair<LinkedList<TreeNode<Node>>, LinkedList<JoinCriteria>> resolvedLeftJoin = resolveJoin(lj);
            res.addAll(resolvedLeftJoin.getKey());
            joinOns.addAll(resolvedLeftJoin.getValue());
        } else {
            TreeNode<Node> ljn = resolveRelation(leftJoin);
            if (ljn != null) {
                res.add(ljn);
            } else {
                logger.warn(String.format("a left Join doesn't contains any valid relation. Join: %s", leftJoin));
            }
        }

        TreeNode<Node> rjn = resolveRelation(join.getRight());
        if (rjn != null) {
            res.add(rjn);
        } else {
            logger.warn(String.format("a right Join doesn't contains any valid relation. Join: %s", join.getRight()));
        }
        return Pair.of(res, joinOns);
    }

    private LinkedList<ColumnMap> resolveSelect(Select select) {
        LinkedList<ColumnMap> res = Lists.newLinkedList();
        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem item : selectItems) {
            if (item instanceof SingleColumn) {
                SingleColumn i = (SingleColumn) item;
                String column = getColumn(i.getExpression());
                String func = getFunctionName(i.getExpression());
                if (StringUtils.isNotEmpty(column)) {
                    if (i.getAlias().isPresent()) {
                        res.add(new ColumnMap(column, i.getAlias().get().toString(), func));
                    } else {
                        if (column.contains(tableColumnSplit)) {
                            String[] split = column.split("\\.");
                            if (split.length < 2) {
                                res.add(new ColumnMap(column, "", func));
                            } else {
                                res.add(new ColumnMap(column, split[1], func));
                            }
                        } else {
                            res.add(new ColumnMap(column, column, func));
                        }
                    }
                }
            } else if (item instanceof AllColumns) {
                // todo need HMS
                AllColumns a = (AllColumns) item;
                if (a.getPrefix().isPresent()) {
                    res.add(new ColumnMap(a.getPrefix().get() + tableColumnSplit + "*",
                            a.getPrefix().get() + tableColumnSplit + "*", null));
                } else {
                    res.add(new ColumnMap("*", "*", null));
                }
            } else {
                //                throw new SqlParseException("unknown column type:" + item.getClass().getName());
                logger.error(String.format("unknown select column type:%s", item));
            }
        }
        return res;
    }

    private String getFunctionName(Expression expression) {
        if (expression instanceof FunctionCall) {
            StringBuilder sb = new StringBuilder();
            FunctionCall fe = (FunctionCall) expression;
            List<String> parts = fe.getName().getParts();
            for (String part : parts) {
                sb.append(part).append(sameSingleColumnSplit);
            }
            for (Expression argument : fe.getArguments()) {
                String argumentFunc = getFunctionName(argument);
                if (StringUtils.isNotEmpty(argumentFunc)) {
                    sb.append(argumentFunc);
                }
            }
            return Arrays.stream(sb.toString().split(sameSingleColumnSplit))
                    .filter(Objects::nonNull)
                    .filter(StringUtils::isNotEmpty)
                    .distinct()
                    .collect(Collectors.joining(sameSingleColumnSplit));
        } else if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return getFunctionName(cast.getExpression());
        } else if (expression instanceof SubscriptExpression) {
            SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
            return getFunctionName(subscriptExpression.getBase());
        } else if (expression instanceof IsNullPredicate) {
            IsNullPredicate isNullExp = (IsNullPredicate) expression;
            return getFunctionName(isNullExp.getValue());
        } else if (expression instanceof IsNotNullPredicate) {
            IsNotNullPredicate notNull = (IsNotNullPredicate) expression;
            return getFunctionName(notNull.getValue());
        } else if (expression instanceof TryExpression) {
            TryExpression te = (TryExpression) expression;
            return getFunctionName(te.getInnerExpression());
        } else if (expression instanceof BetweenPredicate) {
            BetweenPredicate bpe = (BetweenPredicate) expression;
            return getFunctionName(bpe.getValue());
        }
        return null;
    }

    private String getColumn(Expression expression) {
        if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            List<Expression> list = Lists.newArrayList();
            list.add(ifExpression.getCondition());
            list.add(ifExpression.getTrueValue());
            ifExpression.getFalseValue().ifPresent(list::add);
            return getString(list);
        } else if (expression instanceof Identifier) {
            Identifier identifier = (Identifier) expression;
            return identifier.getValue();
        } else if (expression instanceof FunctionCall) {
            FunctionCall fe = (FunctionCall) expression;
            StringBuilder columns = new StringBuilder();
            List<Expression> arguments = fe.getArguments();
            int size = arguments.size();
            for (int i = 0; i < size; i++) {
                Expression exp = arguments.get(i);
                if (i == 0) {
                    columns.append(getColumn(exp));
                } else {
                    columns.append(sameSingleColumnSplit).append(getColumn(exp));
                }
            }
            return Arrays.stream(columns.toString().split(sameSingleColumnSplit)).filter(StringUtils::isNotEmpty)
                    .distinct()
                    .collect(Collectors.joining(sameSingleColumnSplit));
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compare = (ComparisonExpression) expression;
            return getString(compare.getLeft(), compare.getRight());
        } else if (expression instanceof Literal) {
            String s = expression.toString();
            // extract from json
            if (s.startsWith("\'$.")) {
                return s.substring(3, s.length() - 1);
            } else if (s.startsWith("\'$[\"")) {
                return s.substring(4, s.length() - 3);
            } else if (s.startsWith("\'$[")) {
                return s.substring(3, s.length() - 2);
            }
            return "";
        } else if (expression instanceof ArithmeticUnaryExpression) {
            return "";
        } else if (expression instanceof Cast) {
            Cast cast = (Cast) expression;
            return getColumn(cast.getExpression());
        } else if (expression instanceof DereferenceExpression) {
            DereferenceExpression reference = (DereferenceExpression) expression;
            return reference.toString();
        } else if (expression instanceof ArithmeticBinaryExpression) {
            ArithmeticBinaryExpression binaryExpression = (ArithmeticBinaryExpression) expression;
            return getString(binaryExpression.getLeft(), binaryExpression.getRight());
        } else if (expression instanceof SearchedCaseExpression) {
            SearchedCaseExpression caseExpression = (SearchedCaseExpression) expression;
            List<Expression> exps = caseExpression.getWhenClauses().stream().map(whenClause -> (Expression) whenClause)
                    .collect(Collectors.toList());
            caseExpression.getDefaultValue().ifPresent(exps::add);
            return getString(exps);
        } else if (expression instanceof WhenClause) {
            WhenClause whenClause = (WhenClause) expression;
            return getString(whenClause.getOperand(), whenClause.getResult());
        } else if (expression instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) expression;
            return getString(likePredicate.getValue());
        } else if (expression instanceof InPredicate) {
            InPredicate predicate = (InPredicate) expression;
            return predicate.getValue().toString();
        } else if (expression instanceof SubscriptExpression) {
            SubscriptExpression subscriptExpression = (SubscriptExpression) expression;
            return getColumn(subscriptExpression.getBase());
        } else if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicExp = (LogicalBinaryExpression) expression;
            return getString(logicExp.getLeft(), logicExp.getRight());
        } else if (expression instanceof IsNullPredicate) {
            IsNullPredicate isNullExp = (IsNullPredicate) expression;
            return getColumn(isNullExp.getValue());
        } else if (expression instanceof IsNotNullPredicate) {
            IsNotNullPredicate notNull = (IsNotNullPredicate) expression;
            return getColumn(notNull.getValue());
        } else if (expression instanceof CoalesceExpression) {
            CoalesceExpression coalesce = (CoalesceExpression) expression;
            return getString(coalesce.getOperands());
        } else if (expression instanceof AtTimeZone) {
            AtTimeZone atTimeZone = (AtTimeZone) expression;
            return getString(atTimeZone.getValue());
        } else if (expression instanceof CurrentTime) {
            return "";
        } else if (expression instanceof SubqueryExpression) {
            // when a column is a subquery, ignore it. cuz we have another method to process this subquery.
            return "";
        } else if (expression instanceof NullIfExpression) {
            NullIfExpression nullIf = (NullIfExpression) expression;
            return getString(nullIf.getFirst(), nullIf.getSecond());
        } else if (expression instanceof NotExpression) {
            NotExpression ne = (NotExpression) expression;
            return getString(ne.getValue());
        } else if (expression instanceof SimpleCaseExpression) {
            SimpleCaseExpression sce = (SimpleCaseExpression) expression;
            List<Expression> exps = sce.getWhenClauses().stream().map(whenClause -> (Expression) whenClause)
                    .collect(Collectors.toList());
            exps.add(sce.getOperand());
            sce.getDefaultValue().ifPresent(exps::add);
            return getString(exps);
        } else if (expression instanceof TryExpression) {
            TryExpression te = (TryExpression) expression;
            return getColumn(te.getInnerExpression());
        } else if (expression instanceof BetweenPredicate) {
            BetweenPredicate bpe = (BetweenPredicate) expression;
            return getColumn(bpe.getValue());
        } else if (expression instanceof ArrayConstructor) {
            ArrayConstructor ace = (ArrayConstructor) expression;
            return getString(ace.getValues());
        } else if (expression instanceof Extract) {
            Extract ee = (Extract) expression;
            return getString(ee.getExpression());
        } else if (expression instanceof Row) {
            Row re = (Row) expression;
            List<Expression> items = re.getItems();
            return getString(items);
        } else if (expression instanceof LambdaExpression) {
            LambdaExpression le = (LambdaExpression) expression;
            return getString(le.getBody());
        } else if (expression instanceof GroupingOperation) {
            GroupingOperation ge = (GroupingOperation) expression;
            return getString(ge.getGroupingColumns());
        } else if (expression instanceof ExistsPredicate) {
            //            ExistsPredicate ee = (ExistsPredicate) expression;
            return null;//ignore exists(select * from xxx)
        } else if (expression instanceof QuantifiedComparisonExpression) {
            QuantifiedComparisonExpression qce = (QuantifiedComparisonExpression) expression;
            return getString(qce.getValue());
        }
        logger.error("unknown expression:" + expression);
        return null;
    }


    private String getString(Expression... exps) {
        return getString(Arrays.stream(exps).collect(Collectors.toList()));
    }

    private String getString(List<Expression> exps) {
        StringBuilder builder = new StringBuilder();
        for (Expression exp : exps) {
            builder.append(getColumn(exp)).append(sameSingleColumnSplit);
        }
        return Arrays.stream(builder.toString().split(sameSingleColumnSplit)).filter(StringUtils::isNotEmpty).distinct()
                .collect(Collectors.joining(sameSingleColumnSplit));
    }
}

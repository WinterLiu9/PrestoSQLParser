package com.shopee.sqlparser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.WithQuery;

import com.google.common.collect.Sets;
import com.shopee.sqlparser.exceptions.SqlParseException;

class DepthFirstVisitor<R, C> extends AstVisitor<Stream<R>, C> {

    private static Logger logger = LoggerFactory.getLogger(DepthFirstVisitor.class);

    static DepthFirstVisitor<AliasedRelation, ?> aliasVisitor =
            DepthFirstVisitor.by(Extractors.extractAliasedRelation());

    static DepthFirstVisitor<Join, ?> joinVisitor = DepthFirstVisitor.by(Extractors.extractJoin());
    static DepthFirstVisitor<WithQuery, ?> withQueryVisitor = DepthFirstVisitor.by(Extractors.extractWithQuery());
    static DepthFirstVisitor<Table, ?> tableVisitor = DepthFirstVisitor.by(Extractors.extractTable());

    static DepthFirstVisitor<QuerySpecification, ?> querySpecificationVisitor =
            DepthFirstVisitor.by(Extractors.extractQuerySpecification());

    static Map<Relation, List<String>> tablesColumnsMap = new HashMap<>();

    static Map<Table, String> tableAlias = new HashMap<>();

    static List<List<String>> allJoinedTables = new ArrayList<>();

    static final String columnSplit = ",";
    private final AstVisitor<R, C> visitor;

    public DepthFirstVisitor(AstVisitor<R, C> visitor) {
        this.visitor = visitor;
    }

    public static <R, C> DepthFirstVisitor<R, C> by(AstVisitor<R, C> visitor) {
        return new DepthFirstVisitor<>(visitor);
    }

    @Override
    public final Stream<R> visitNode(Node node, C context) {
        Stream<R> nodeResult = Stream.of(visitor.process(node, context));
        Stream<R> childrenResult = node.getChildren().stream()
                .flatMap(child -> process(child, context));

        return Stream.concat(nodeResult, childrenResult)
                .filter(Objects::nonNull);
    }

    public static void cacheTableAlias(Statement statement) {
        List<AliasedRelation> alias =
                statement.accept(aliasVisitor, null).filter(Objects::nonNull).collect(Collectors.toList());
        for (AliasedRelation a : alias) {
            Table table = (Table) a.getRelation();
            String value = a.getAlias().getValue();

            if (tableAlias.containsKey(table)) {
                String alyAlias = tableAlias.get(table);
                if (!alyAlias.equals(value)) {
                    // todo alias conflict
                    logger.warn("Multiple aliases for the same table. Table name is: " + table.getName().toString()
                            + ". alias:" + alyAlias + " and " + value);
                }
            }
            tableAlias.put(table, value);
        }
    }

    private static void cacheTableJoin(Statement statement) {
        List<Join> joins = statement.accept(joinVisitor, null).filter(Objects::nonNull).collect(Collectors.toList());
        for (Join join : joins) {
            List<String> joinedTables =
                    join.accept(tableVisitor, null).filter(Objects::nonNull).map(x -> x.getName().toString())
                            .collect(Collectors.toList());
            allJoinedTables.add(joinedTables);
        }
    }

    private static String readSQL() {
        try {
            File myObj = new File("./src/main/java/com/shopee/sqlparser/t.sql");
            Scanner myReader = new Scanner(myObj);
            StringBuffer sb = new StringBuffer();
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                sb.append("\n");
                sb.append(data);
            }
            myReader.close();
            return sb.toString();
        } catch (FileNotFoundException e) {
            logger.error("file not found exception", e);
            e.printStackTrace();
            return "";
        }
    }

    public static void main(String[] args) throws SqlParseException {
        String sql = readSQL();

        Statement statement =
                new SqlParser().createStatement(sql, new ParsingOptions(DecimalLiteralTreatment.AS_DOUBLE));

        cacheTableAlias(statement);
        cacheTableJoin(statement);


        List<WithQuery> withQueries = statement.accept(withQueryVisitor, null).collect(Collectors.toList());
        Set<QuerySpecification> withQuerySpecifications = Sets.newHashSet();
        for (WithQuery withQuery : withQueries) {
            withQuerySpecifications.addAll(
                    withQuery.getQuery().accept(querySpecificationVisitor, null).collect(Collectors.toList()));
        }

        List<QuerySpecification> querySpecifications =
                statement.accept(querySpecificationVisitor, null)
                        .filter(x -> !withQuerySpecifications.contains(x))
                        .collect(Collectors.toList());


        for (QuerySpecification querySpecification : querySpecifications) {
            //            List<QuerySpecification> subQuery = querySpecification.getFrom().orElse(null).accept
            //            (querySpecificationVisitor, null).collect(Collectors.toList());
            exploreQuery(querySpecification);
        }


        Map<String, String> withMap = new HashMap<>();
        for (WithQuery withQuery : withQueries) {
            //            withMap.put(withQuery.getName().getValue(), "")
        }
        System.out.println(tablesColumnsMap);
    }

    private static String addTableName(String column) throws SqlParseException {
        if (column.contains(".")) {
            String[] split = column.split("\\.");
            if (split.length == 2) { // table.column
                String t = split[0];
                String c = split[1];
                return tableAlias.getOrDefault(t, t) + "." + c;
            } else if (split.length == 3) { // schema.table.column
                return column;
            } else {
                throw new SqlParseException("un known column name");
            }
        } else { // only has column name
            // todo add table name
            return column;
        }
    }

    public static void exploreQuery(QuerySpecification node) throws SqlParseException {
        if (node == null) {
            return;
        }
        // select 嵌套
        if (node.getFrom().orElse(null) != null) {
            List<String> columns =
                    extractColumns(node.getSelect()).stream().map(x -> {
                        try {
                            return addTableName(x);
                        } catch (SqlParseException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
            tablesColumnsMap.put(node.getFrom().get(), columns);
        }

    }

    private static List<String> extractColumns(Select select) throws SqlParseException {
        List<SelectItem> selectItems = select.getSelectItems();
        List<String> columns = new ArrayList<>();
        for (SelectItem item : selectItems) {
            if (item instanceof SingleColumn) {
                columns.add(getColumn(((SingleColumn) item).getExpression()));
            } else if (item instanceof AllColumns) {
                columns.add(item.toString());// todo
            } else {
                throw new SqlParseException("unknown column type:" + item.getClass().getName());
            }
        }
        return Arrays.stream(String.join(",", columns).split(",")).filter(StringUtils::isNotEmpty)
                .collect(Collectors.toList());

    }

    private static String getColumn(Expression expression) throws SqlParseException {
        if (expression instanceof IfExpression) {
            IfExpression ifExpression = (IfExpression) expression;
            List<Expression> list = new ArrayList<>();
            list.add(ifExpression.getCondition());
            list.add(ifExpression.getTrueValue());
            ifExpression.getFalseValue().ifPresent(list::add);
            return getString(list);
        } else if (expression instanceof Identifier) {
            Identifier identifier = (Identifier) expression;
            return identifier.getValue();
        } else if (expression instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) expression;
            StringBuilder columns = new StringBuilder();
            List<Expression> arguments = call.getArguments();
            int size = arguments.size();
            for (int i = 0; i < size; i++) {
                Expression exp = arguments.get(i);
                if (i == 0) {
                    columns.append(getColumn(exp));
                } else {
                    columns.append(getColumn(exp)).append(columnSplit);
                }
            }
            return columns.toString();
        } else if (expression instanceof ComparisonExpression) {
            ComparisonExpression compare = (ComparisonExpression) expression;
            return getString(compare.getLeft(), compare.getRight());
        } else if (expression instanceof Literal) {
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
            return likePredicate.getValue().toString();
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
            return null;
        } else if (expression instanceof SubqueryExpression) {
            // when a column is a subquery, ignore it. cuz we have another method to process this subquery.
            return null;
        }
        System.out.println(expression);
        throw new SqlParseException("unknown expression type:" + expression.getClass().getName());
    }


    private static String getString(Expression... exps) throws SqlParseException {
        return getString(Arrays.stream(exps).collect(Collectors.toList()));
    }

    private static String getString(List<Expression> exps) throws SqlParseException {
        StringBuilder builder = new StringBuilder();
        for (Expression exp : exps) {
            builder.append(getColumn(exp)).append(columnSplit);
        }
        return builder.toString();
    }

}




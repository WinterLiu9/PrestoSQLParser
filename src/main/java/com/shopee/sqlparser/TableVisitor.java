package com.shopee.sqlparser;

import java.util.Objects;
import java.util.stream.Stream;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Node;

class TableVisitor<R, C> extends AstVisitor<Stream<R>, C> {
    private final AstVisitor<R, C> visitor;

    public TableVisitor(AstVisitor<R, C> visitor) {
        this.visitor = visitor;
    }

    public static <R, C> TableVisitor<R, C> by(AstVisitor<R, C> visitor) {
        return new TableVisitor<>(visitor);
    }

    @Override
    public final Stream<R> visitNode(Node node, C context) {
        Stream<R> nodeResult = Stream.of(visitor.process(node, context));
        Stream<R> childrenResult = node.getChildren().stream()
                .flatMap(child -> process(child, context));

        return Stream.concat(nodeResult, childrenResult)
                .filter(Objects::nonNull);
    }
}
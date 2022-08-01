package com.shopee.sqlparser;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;

public class Extractors {
    public static AstVisitor<WithQuery, Object> extractWithQuery() {
        return new AstVisitor<WithQuery, Object>() {
            @Override
            protected WithQuery visitWithQuery(WithQuery node, Object context) {
                return node;
            }
        };
    }

    public static AstVisitor<QuerySpecification, Integer> extractQuerySpecification() {
        return new AstVisitor<QuerySpecification, Integer>() {
            @Override
            protected QuerySpecification visitQuerySpecification(QuerySpecification node, Integer level) {
                return node;
            }
        };
    }


    public static AstVisitor<Table, Object> extractTable() {
        return new AstVisitor<Table, Object>() {
            @Override
            protected Table visitTable(Table node, Object context) {
                return node;
            }
        };
    }

    public static AstVisitor<AliasedRelation, Object> extractAliasedRelation() {
        return new AstVisitor<AliasedRelation, Object>() {
            @Override
            protected AliasedRelation visitAliasedRelation(AliasedRelation node, Object context) {
                if (node.getRelation() instanceof Table) {
                    return node;
//                    return ((Table) node.getRelation()).getName().toString() + "," + node.getAlias().getValue();
                }
                return null;
            }
        };
    }

    public static AstVisitor<Join, Object> extractJoin() {
        return new AstVisitor<Join, Object>() {
            @Override
            protected Join visitJoin(Join node, Object context) {
                return node;
            }
        };
    }
}

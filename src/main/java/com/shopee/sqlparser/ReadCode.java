package com.shopee.sqlparser;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;

public class ReadCode {
    private static Logger logger = LoggerFactory.getLogger(ReadCode.class);

    // add schema on table https://stackoverflow.com/questions/65052760/sql-parser-visitor-metabase-presto
    public static class SchemaAwareQueryVisitor extends DefaultTraversalVisitor<Void, Void> {
        private String schemaId;

        public SchemaAwareQueryVisitor(String schemaId) {
            super();
            this.schemaId = schemaId;
        }

        @Override
        protected Void visitTable(Table node, Void context) {
            try {
                Field privateStringField = Table.class.getDeclaredField("name");
                privateStringField.setAccessible(true);
                QualifiedName qualifiedName = QualifiedName.of(schemaId, node.getName().getParts().get(0));
                privateStringField.set(node, qualifiedName);
            } catch (NoSuchFieldException
                     | SecurityException
                     | IllegalArgumentException
                     | IllegalAccessException e) {
                throw new SecurityException("Unable to execute query");
            }
            return null;
        }
    }


    public static void main(String[] args) {
        //        SQLParser sqlParser = new SQLParser(sql, DbType.presto);
        String arrTest = "SELECT ARRAY[date'2021-01-01',date'2021-02-12'] ";
        logger.info("hello");
    }

    static String s1 = "";

    static String arrTest = "SELECT ARRAY[date'2021-01-01',date '2021-02-12'] AS public_holidays";

    static String aliasAndJsonTest =
            "select json_extract(json, '$.store.book'), orderid as order_id, shopid as shop_id from test.db t";
}
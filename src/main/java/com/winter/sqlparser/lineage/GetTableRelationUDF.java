package com.winter.sqlparser.lineage;

import java.util.List;

import com.winter.sqlparser.tree.ExtractQueryTree;
import com.winter.sqlparser.tree.TreeNode;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.parser.StatementSplitter;
import com.facebook.presto.sql.tree.Node;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.winter.sqlparser.Parser;


public class GetTableRelationUDF extends UDF {

    private static final Logger logger = LoggerFactory.getLogger(GetTableRelationUDF.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public String evaluate(String sql) {
        try {
            List<TableRelation> relations = Lists.newLinkedList();
            List<StatementSplitter.Statement> statements = Parser.getStatements(sql);
            for (int i = 0; i < statements.size(); i++) {
                ExtractQueryTree tree = new ExtractQueryTree(Parser.parse(statements.get(i)));
                TreeNode<Node> queryTree = tree.getQueryTree(i);
                if (queryTree == null) {
                    continue;
                }
                Lineage lineage = new Lineage(queryTree);
                List<TableRelation> l = lineage.getTableRelation();
                relations.addAll(l);
            }
            return mapper.writeValueAsString(relations);
        } catch (Exception e) {
            return null;
        }
    }
}

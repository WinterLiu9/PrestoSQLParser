package com.winter.sqlparser;

import java.util.List;

import com.winter.sqlparser.tree.ExtractQueryTree;
import com.winter.sqlparser.tree.TreeNode;
import com.winter.sqlparser.util.FileUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.winter.sqlparser.lineage.ColumnMap;
import com.winter.sqlparser.lineage.Lineage;
import com.winter.sqlparser.lineage.TableRelation;

public class TestExtractQueryTree {
    private static final Logger logger = LoggerFactory.getLogger(TestExtractQueryTree.class);


    String sql;

    @Before
    public void init() throws SqlParseException {
        String path2SQL = "./src/test/resources/sql/test.sql";
        sql = FileUtil.readSQL(path2SQL);
    }

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void test() throws Exception {
        List<StatementSplitter.Statement> statements = Parser.getStatements(sql);
        for (StatementSplitter.Statement statement : statements) {
            Statement parsed = null;
            try {
                parsed = Parser.parse(statement);
            } catch (ParsingException pe) {
                logger.error("here is a ParsingException");
                return;
            }
            List<TableRelation> res = Lists.newArrayList();
            ExtractQueryTree extractQueryTree = new ExtractQueryTree(parsed);
            TreeNode<Node> root = extractQueryTree.getQueryTree(0);
            Lineage tl = new Lineage(root);
            List<TableRelation> l = tl.getTableRelation();

           for (TableRelation tableRelation : l) {
               if (tableRelation.getRelation().equals("JOIN")) {
                   System.out.println(tableRelation);

               }
           }
           System.out.println();
           for (TableRelation tableRelation : l) {
               if (tableRelation.getRelation().equals("UNION")) {
                   System.out.println(tableRelation);
               }
           }
            System.out.println();
            for (TableRelation tableRelation : l) {
                if (tableRelation.getRelation().equals("PARENT_CHILD")) {
                    System.out.println(tableRelation);
                }
            }

            for (TableRelation tableRelation : l) {
                for (ColumnMap columnMap : CollectionUtils.emptyIfNull(tableRelation.getMapList())) {
                    System.out.println(columnMap);
                }
            }
        }
    }

}

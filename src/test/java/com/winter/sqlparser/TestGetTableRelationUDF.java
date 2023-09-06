package com.winter.sqlparser;

import java.util.Collections;
import java.util.List;

import com.winter.sqlparser.util.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.presto.sql.parser.ParsingException;
import com.google.common.collect.Lists;
import com.winter.sqlparser.lineage.GetTableRelationUDF;

import jodd.util.StringUtil;

public class TestGetTableRelationUDF {

    private static final Logger logger = LoggerFactory.getLogger(TestGetTableRelationUDF.class);
    List<String> sqls;

    @Before
    public void init() throws Exception {
//        String path2SQL = "./src/test/resources/sql/excel_929.xlsx";
//        sqls = FileUtil.readExcel(path2SQL, 999999);
        String path2SQL = "./src/test/resources/sql/5.sql";
        sqls = Collections.singletonList(FileUtil.readSQL(path2SQL));
    }


    @Test
    public void testUDF() throws Exception {
        String sqlp = null;
        try{
            GetTableRelationUDF gtr = new GetTableRelationUDF();
            List<String> err = Lists.newLinkedList();
            int i = 0, j = 0, index = 0;
            for (String sql : sqls) {
                index++;
                try {
                    sqlp = sql;
                    String evaluate = gtr.evaluate(sql);
                    if (StringUtil.isNotEmpty(evaluate)) {
                        System.out.println(evaluate);
                        System.out.println(i++);
                        i++;
                    } else {
                        j++;
                    }
                } catch (ParsingException pe) {
                    err.add(sql);
                    j++;
                }
            }
            System.err.println("i=" + i);
            System.err.println("j=" + i);
            System.out.println("index=" + index);
        } catch (StackOverflowError stackOverflowError) {
            logger.error(sqlp);
        }
    }

}

//package com.winter.sqlparser.dao;
//
//import java.util.List;
//import java.util.Map;
//
//import org.apache.commons.lang3.StringUtils;
//import org.neo4j.driver.AuthTokens;
//import org.neo4j.driver.Config;
//import org.neo4j.driver.Driver;
//import org.neo4j.driver.GraphDatabase;
//import org.neo4j.driver.Record;
//import org.neo4j.driver.Result;
//import org.neo4j.driver.Session;
//import org.neo4j.driver.SessionConfig;
//import org.neo4j.driver.exceptions.Neo4jException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.winter.sqlparser.SqlParseException;
//
//
//public class Neo4jDao implements AutoCloseable {
//    private static final Logger logger = LoggerFactory.getLogger(Neo4jDao.class);
//    private final Driver driver;
//
//    public Neo4jDao() {
//        String uri = "neo4j+s://6711c13a.databases.neo4j.io";
//        String user = "neo4j";
//        String password = "HRSqiFtFerkPyVlw0Dl4x0o0UHjbv1IrosvKIRpFpP8";
//        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), Config.defaultConfig());
//    }
//
//    public void createColumnIfNotExist(List<String> columns, String table) throws SqlParseException {
//        StringBuilder sb = new StringBuilder();
//        sb.append("MATCH (t:Table {name: $table_name})\n");
//        for (int i = 0; i < columns.size(); i++) {
//            sb.append(String.format("MERGE (c%s: Column {name: $column%s_name}) \n", i, i));
//            sb.append(String.format("MERGE ( c%s )-[r%s: BELONGS] -> (t) \n", i,i));
//        }
//        sb.append("RETURN t");
//        Map<String, Object> params = Maps.newLinkedHashMap();
//        params.put("table_name", table);
//        for (int i = 0; i < columns.size(); i++) {
//            params.put(String.format("column%s_name", i), columns.get(i));
//        }
//        this.create(sb.toString(), params);
//    }
//
//    public void createTable(String name) throws SqlParseException {
//        String createTable = "CREATE (t1:Table {  name: $table_name })\n" +
//                "RETURN t1";
//        Map<String, Object> params = Maps.newHashMap();
//        params.put("table_name", name);
//        this.create(createTable, params);
//    }
//
//    public void createTableIfNotExists(String name) throws SqlParseException {
//        if (StringUtils.isEmpty(name)) return;
//        String createTable = "MERGE (t1:Table {  name: $table_name })\n" +
//                "RETURN t1";
//        Map<String, Object> params = Maps.newHashMap();
//        params.put("table_name", name);
//        this.create(createTable, params);
//    }
//
//    public List<String> queryTable(String name) {
//        String queryTable = "MATCH (t1:Table {  name: $table_name }) RETURN t1";
//        Map<String, Object> params = Maps.newHashMap();
//        params.put("table_name", name);
//        return this.query(queryTable, params);
//    }
//
//    public void createJoinLineage(List<String> tables) {
//        StringBuilder sb = new StringBuilder();
//        String f = "MATCH (t%s:Table {name: $table%s_name})\n";
//        for (int i = 0; i < tables.size(); i++) {
//            sb.append(String.format(f, i, i));
//        }
//        for (int i = 0; i < tables.size(); i++) {
//            sb.append(String.format("MERGE (t%s)-[r%s:JOIN]->(t%s) \n", i,i, i+1));
//        }
//        sb.append("RETURN r0");
//        Map<String, Object> params = Maps.newLinkedHashMap();
//        for (int i = 0; i < tables.size(); i++) {
//            params.put(String.format("table%s_name", i), tables.get(i));
//        }
//        System.out.println(params);
//        this.create(sb.toString(), params);
//    }
//
//    public void createTableLineage(String name1, String name2) {
//        String createLineage = "MATCH (t1:Table {name: $name1}) MATCH (t2:Table {name: $name2}) \n"
//                + "MERGE (t1)-[r:TO]->(t2)\n"
//                + "RETURN r";
//        Map<String, Object> params = Maps.newHashMap();
//        params.put("name1", name1);
//        params.put("name2", name2);
//        this.create(createLineage, params);
//    }
//
//    public void create(String cypher, Map<String, Object> params) {
//        try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
//            List<Record> record = session.writeTransaction(tx -> {
//                Result result = tx.run(cypher, params);
//                return result.list();
//            });
//        } catch (Neo4jException ex) {
//            logger.error(String.format("create :\n%s\n raised an exception: %s", cypher, ex));
//        }
//    }
//
//    public List<String> query(String cypher, Map<String, Object> params) {
//        try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
//            List<String> list = session.readTransaction(tx -> {
//                List<String> res = Lists.newArrayList();
//                Result result = tx.run(cypher, params);
//                while (result.hasNext()) {
//                    res.add(result.next().toString());
//                }
//                return res;
//            });
//            return list;
//        } catch (Neo4jException ex) {
//            logger.error(String.format("query:\n%s\n raised an exception: %s", cypher, ex));
//            return null;
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        driver.close();
//    }
//}

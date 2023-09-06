package com.winter.sqlparser;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

import java.util.List;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.StatementSplitter;
import com.facebook.presto.sql.tree.Statement;

/**
 * wrapper for the parsing functionality provided by Facebook's presto-parser.
 * Simplifies the API for parsing and printing SQL/ASTs.
 */
public class Parser {

    private static final SqlParser sqlParser = new SqlParser();
    private static final ParsingOptions parsingOptions = sivtParsingOptions();

    private static final String regex = "[\\t\\r\\n]|(--[^\\r\\n]*)|(/\\*[\\w\\W]*?(?=\\*/)\\*/)";

    /**
     * Parse an SQL statement.
     *
     * @param statement The statement to be parsed. This statement must be one of the statements
     * returned by the StatementSplitter.
     * @return The parsed statement.
     */
    public static Statement parse(StatementSplitter.Statement statement) {
        return sqlParser.createStatement(statement.statement(), parsingOptions);
    }

    /**
     * Get the individual SQL statements from a string of concatenated SQL statements.
     *
     * @param sql The concatenated SQL statements.
     * @return List of the individual statements.
     */

    public static List<StatementSplitter.Statement> getStatements(String sql) {
        String s = StatementSplitter.squeezeStatement(sql.replaceAll(regex, "\n")); // delete comment
        StatementSplitter statementSplitter = new StatementSplitter(s + "\n;");
        return statementSplitter.getCompleteStatements();
    }


    /**
     * Get the default parsing options.
     *
     * @return The default parsing options fit for the parsing Propic SQL scripts.
     */
    private static ParsingOptions sivtParsingOptions() {
        ParsingOptions.Builder parsingOptionsBuilder = ParsingOptions.builder();
        parsingOptionsBuilder.setDecimalLiteralTreatment(AS_DECIMAL);
        return parsingOptionsBuilder.build();
    }
}

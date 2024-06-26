package org.kenstott;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;


/**
 * The SQLiteSqlDialect class is a subclass of SqlDialect that provides support for SQLite database.
 *
 * The only difference with SQLite and the default Calcite dialect (that was identified) was the lack of support
 * for FETCH NEXT ROWS syntax. There may be other differences. As they are discovered this is
 * the class to use to make changes to rewriting the query.
 */
public class SQLiteSqlDialect extends SqlDialect {
    /**
     * Creates a new instance of SQLiteSqlDialect.
     *
     * @param context the context for creating the dialect
     */
    public SQLiteSqlDialect(Context context) {
        super(context);
    }

    /**
     * This method unparses the LIMIT/OFFSET clause in SQL for SQLite into
     * an LIMIT OFFSET syntax. The default rewrites this using FETCH NEXT ROWS.
     *
     * @param writer The writer to output the SQL
     * @param offset The SQL node representing the offset value
     * @param fetch The SQL node representing the fetch value
     */
    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }
}

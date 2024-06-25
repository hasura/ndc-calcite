package org.kenstott;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SQLiteSqlDialect extends SqlDialect {
    public SQLiteSqlDialect(Context context) {
        super(context);
    }

    @Override
    public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }
}

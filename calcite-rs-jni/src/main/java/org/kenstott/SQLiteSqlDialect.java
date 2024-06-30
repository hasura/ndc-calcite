package org.kenstott;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.util.Util;

import java.util.Iterator;
import java.util.List;


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


    /**
     * This method unparses a SqlCall into SQL and writes it to the SqlWriter.
     *
     * If the operator of the SqlCall is "JSON_OBJECT", it uses a special unparse logic to handle the call.
     * It starts a JSON_OBJECT function call frame, followed by a list frame.
     * Then, it iterates over the operands of the call (starting from the second operand) and unparse them,
     * alternating between the key and value operands.
     * Finally, it ends the list frame and the function call frame.
     *
     * If the operator of the SqlCall is not "JSON_OBJECT", it delegates the unparse logic to the super class,
     * which handles the unparse for other functions.
     *
     * @param writer The SqlWriter to write the SQL
     * @param call The SqlCall to unparse
     * @param leftPrec The left precedence of the parent operator
     * @param rightPrec The right precedence of the parent operator
     */
    @Override
    public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        System.out.println(call.getOperator().getName());
        if (call.getOperator().getName().equalsIgnoreCase("JSON_OBJECT")) {
            SqlWriter.Frame frame = writer.startFunCall("JSON_OBJECT");
            SqlWriter.Frame listFrame = writer.startList("", "");
            for (int i = 1; i < call.operandCount(); i += 2) {
                writer.sep(",");
                call.operand(i).unparse(writer, leftPrec, rightPrec);
                writer.sep(",");
                call.operand(i + 1).unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(listFrame);
            writer.endFunCall(frame);
        } else if (call.getOperator().getName().equalsIgnoreCase("JSON_ARRAYAGG")) {
            SqlWriter.Frame frame = writer.startFunCall("JSON_GROUP_ARRAY");
            SqlWriter.Frame listFrame = writer.startList("", "");
            for (int i = 1; i < call.operandCount(); i += 2) {
                writer.sep(",");
                call.operand(i).unparse(writer, leftPrec, rightPrec);
                writer.sep(",");
                call.operand(i + 1).unparse(writer, leftPrec, rightPrec);
            }
            writer.endList(listFrame);
            writer.endFunCall(frame);
        } else if ((call.getOperator().getName().equalsIgnoreCase("CAST"))) {
            SqlWriter.Frame frame = writer.startFunCall("CAST");
            call.operand(0).unparse(writer, 0, 0);
            writer.sep("AS");
            if (call.operand(1) instanceof SqlIntervalQualifier) {
                writer.sep("INTERVAL");
            }

            call.operand(1).unparse(writer, 0, 0);
            if (call.getOperandList().size() > 2) {
                writer.sep("FORMAT");
                call.operand(2).unparse(writer, 0, 0);
            }

            writer.endFunCall(frame);

        } else {
            // Default behavior for other functions
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }

    @Override
    public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
        System.out.println(operator.getName());
        Boolean result = super.supportsFunction(operator, type, paramTypes);
        return result;
    }

    @Override
    public boolean supportsCharSet() {
        return false;
    }
}

package org.kenstott;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.DatabaseMetaData;
import java.util.Objects;

/**
 * A factory for creating a SQLite SQL dialect.
 */
public class SQLiteSqlDialectFactory implements SqlDialectFactory {

    private static class ContextImpl implements SqlDialect.Context {
        private final SqlDialect.DatabaseProduct databaseProduct;
        private final @Nullable String databaseProductName;
        private final @Nullable String databaseVersion;
        private final int databaseMajorVersion;
        private final int databaseMinorVersion;
        private final String literalQuoteString;
        private final String literalEscapedQuoteString;
        private final @Nullable String identifierQuoteString;
        private final @Nullable String identifierEscapedQuoteString;
        private final Casing quotedCasing;
        private final Casing unquotedCasing;
        private final boolean caseSensitive;
        private final SqlConformance conformance;
        private final NullCollation nullCollation;
        private final RelDataTypeSystem dataTypeSystem;
        private final JethroDataSqlDialect.JethroInfo jethroInfo;

        private ContextImpl(SqlDialect.DatabaseProduct databaseProduct, @Nullable String databaseProductName,
                            @Nullable String databaseVersion, int databaseMajorVersion, int databaseMinorVersion,
                            String literalQuoteString, String literalEscapedQuoteString,
                            @Nullable String identifierQuoteString, @Nullable String identifierEscapedQuoteString,
                            Casing quotedCasing, Casing unquotedCasing, boolean caseSensitive,
                            SqlConformance conformance, NullCollation nullCollation,
                            RelDataTypeSystem dataTypeSystem,
                            JethroDataSqlDialect.JethroInfo jethroInfo) {
            this.databaseProduct = Objects.requireNonNull(databaseProduct, "databaseProduct");
            this.databaseProductName = databaseProductName;
            this.databaseVersion = databaseVersion;
            this.databaseMajorVersion = databaseMajorVersion;
            this.databaseMinorVersion = databaseMinorVersion;
            this.literalQuoteString = literalQuoteString;
            this.literalEscapedQuoteString = literalEscapedQuoteString;
            this.identifierQuoteString = identifierQuoteString;
            this.identifierEscapedQuoteString = identifierEscapedQuoteString;
            this.quotedCasing = Objects.requireNonNull(quotedCasing, "quotedCasing");
            this.unquotedCasing = Objects.requireNonNull(unquotedCasing, "unquotedCasing");
            this.caseSensitive = caseSensitive;
            this.conformance = Objects.requireNonNull(conformance, "conformance");
            this.nullCollation = Objects.requireNonNull(nullCollation, "nullCollation");
            this.dataTypeSystem = Objects.requireNonNull(dataTypeSystem, "dataTypeSystem");
            this.jethroInfo = Objects.requireNonNull(jethroInfo, "jethroInfo");
        }

        @Override
        public SqlDialect.DatabaseProduct databaseProduct() {
            return databaseProduct;
        }

        @Override
        public @Nullable String databaseProductName() {
            return databaseProductName;
        }

        @Override
        public @Nullable String databaseVersion() {
            return databaseVersion;
        }

        @Override
        public int databaseMajorVersion() {
            return databaseMajorVersion;
        }

        @Override
        public int databaseMinorVersion() {
            return databaseMinorVersion;
        }

        @Override
        public String literalQuoteString() {
            return literalQuoteString;
        }

        @Override
        public String literalEscapedQuoteString() {
            return literalEscapedQuoteString;
        }

        @Override
        public @Nullable String identifierQuoteString() {
            return identifierQuoteString;
        }

        @Override
        public @Nullable String identifierEscapedQuoteString() {
            return identifierEscapedQuoteString;
        }

        @Override
        public Casing unquotedCasing() {
            return unquotedCasing;
        }

        @Override
        public Casing quotedCasing() {
            return quotedCasing;
        }

        @Override
        public boolean caseSensitive() {
            return caseSensitive;
        }

        @Override
        public SqlConformance conformance() {
            return conformance;
        }

        @Override
        public NullCollation nullCollation() {
            return nullCollation;
        }

        @Override
        public RelDataTypeSystem dataTypeSystem() {
            return dataTypeSystem;
        }

        @Override
        public JethroDataSqlDialect.JethroInfo jethroInfo() {
            return jethroInfo;
        }

        public SqlDialect.Context withDatabaseProduct(SqlDialect.DatabaseProduct databaseProduct) {
            return new ContextImpl(databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withDatabaseProductName(String databaseProductName) {
            return new ContextImpl(this.databaseProduct, databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withDatabaseVersion(String databaseVersion) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withDatabaseMajorVersion(int databaseMajorVersion) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withDatabaseMinorVersion(int databaseMinorVersion) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withLiteralQuoteString(String literalQuoteString) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withLiteralEscapedQuoteString(String literalEscapedQuoteString) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withIdentifierQuoteString(@Nullable String identifierQuoteString) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withIdentifierEscapedQuoteString(@Nullable String identifierEscapedQuoteString) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withUnquotedCasing(Casing unquotedCasing) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withQuotedCasing(Casing quotedCasing) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withCaseSensitive(boolean caseSensitive) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withConformance(SqlConformance conformance) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, conformance, this.nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withNullCollation(NullCollation nullCollation) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, nullCollation, this.dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withDataTypeSystem(RelDataTypeSystem dataTypeSystem) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, dataTypeSystem, this.jethroInfo);
        }

        public SqlDialect.Context withJethroInfo(JethroDataSqlDialect.JethroInfo jethroInfo) {
            return new ContextImpl(this.databaseProduct, this.databaseProductName, this.databaseVersion, this.databaseMajorVersion, this.databaseMinorVersion, this.literalQuoteString, this.literalEscapedQuoteString, this.identifierQuoteString, this.identifierEscapedQuoteString, this.quotedCasing, this.unquotedCasing, this.caseSensitive, this.conformance, this.nullCollation, this.dataTypeSystem, jethroInfo);
        }


        public @Nullable String getDatabaseProductName() {
            return databaseProductName;
        }


        public @Nullable String getDatabaseVersion() {
            return databaseVersion;
        }


        public int getDatabaseMajorVersion() {
            return databaseMajorVersion;
        }


        public int getDatabaseMinorVersion() {
            return databaseMinorVersion;
        }


        public String getLiteralQuoteString() {
            return literalQuoteString;
        }


        public String getLiteralEscapedQuoteString() {
            return literalEscapedQuoteString;
        }


        public @Nullable String getIdentifierQuoteString() {
            return identifierQuoteString;
        }


        public @Nullable String getIdentifierEscapedQuoteString() {
            return identifierEscapedQuoteString;
        }


        public Casing getUnquotedCasing() {
            return unquotedCasing;
        }


        public Casing getQuotedCasing() {
            return quotedCasing;
        }


        public boolean isCaseSensitive() {
            return caseSensitive;
        }


        public SqlConformance getConformance() {
            return conformance;
        }


        public NullCollation getNullCollation() {
            return nullCollation;
        }


        public RelDataTypeSystem getDataTypeSystem() {
            return dataTypeSystem;
        }


        public JethroDataSqlDialect.JethroInfo getJethroInfo() {
            return jethroInfo;
        }
    }

    private static SqlDialect.Context context() {
        return new ContextImpl(SqlDialect.DatabaseProduct.MYSQL, null, null, -1, -1, "'", "''", "\"", null,
                Casing.UNCHANGED, Casing.TO_UPPER, true, SqlConformanceEnum.DEFAULT,
                NullCollation.HIGH, RelDataTypeSystemImpl.DEFAULT, JethroDataSqlDialect.JethroInfo.EMPTY);
    }

    @Override
    public SqlDialect create(DatabaseMetaData databaseMetaData) {
        return new SQLiteSqlDialect(context());
    }
}
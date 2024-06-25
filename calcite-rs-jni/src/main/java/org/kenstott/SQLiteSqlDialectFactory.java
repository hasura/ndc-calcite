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

public class SQLiteSqlDialectFactory implements SqlDialectFactory {

    private record ContextImpl(SqlDialect.DatabaseProduct databaseProduct, @Nullable String databaseProductName,
                               @Nullable String databaseVersion, int databaseMajorVersion, int databaseMinorVersion,
                               String literalQuoteString, String literalEscapedQuoteString,
                               @Nullable String identifierQuoteString, @Nullable String identifierEscapedQuoteString,
                               Casing quotedCasing, Casing unquotedCasing, boolean caseSensitive,
                               SqlConformance conformance, NullCollation nullCollation,
                               RelDataTypeSystem dataTypeSystem,
                               JethroDataSqlDialect.JethroInfo jethroInfo) implements SqlDialect.Context {
            private ContextImpl(SqlDialect.DatabaseProduct databaseProduct, @Nullable String databaseProductName, @Nullable String databaseVersion, int databaseMajorVersion, int databaseMinorVersion, String literalQuoteString, String literalEscapedQuoteString, @Nullable String identifierQuoteString, @Nullable String identifierEscapedQuoteString, Casing quotedCasing, Casing unquotedCasing, boolean caseSensitive, SqlConformance conformance, NullCollation nullCollation, RelDataTypeSystem dataTypeSystem, JethroDataSqlDialect.JethroInfo jethroInfo) {
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
        }

    private static SqlDialect.Context context() {
        return new ContextImpl(SqlDialect.DatabaseProduct.UNKNOWN, null, null, -1, -1, "'", "''", null, null, Casing.UNCHANGED, Casing.TO_UPPER, true, SqlConformanceEnum.DEFAULT, NullCollation.HIGH, RelDataTypeSystemImpl.DEFAULT, JethroDataSqlDialect.JethroInfo.EMPTY);
    }

    @Override
    public SqlDialect create(DatabaseMetaData databaseMetaData) {
        return new SQLiteSqlDialect(context());
    }
}
/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Teradata database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class TeradataDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            TeradataDialect.class,
            DatabaseProduct.TERADATA);

    /**
     * Creates a TeradataDialect.
     *
     * @param connection Connection
     */
    public TeradataDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        String fromClause = null;
        if (valueList.size() > 1) {
            // In Teradata, "SELECT 1,2" is valid but "SELECT 1,2 UNION
            // SELECT 3,4" gives "3888: SELECT for a UNION,INTERSECT or
            // MINUS must reference a table."
            fromClause = " FROM (SELECT 1 a) z ";
        }
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, fromClause, true);
    }

    protected String guessIntType(
        String basicType,
        List<String[]> valueList,
        int column)
    {
        Datatype type = Datatype.valueOf(basicType);
        for (String[] values : valueList) {
            final String value = values[column];
            if (value == null) {
                continue;
            }
            if (type == Datatype.Integer
                && Long.valueOf(value) > Integer.MAX_VALUE)
            {
                return "BIGINT";
            }
        }
        return "INTEGER";
    }

    public boolean supportsGroupingSets() {
        return true;
    }

    public boolean requiresUnionOrderByOrdinal() {
        return true;
    }

    public boolean supportsOrderInSubqueries() {
        return false;
    }
}

// End TeradataDialect.java

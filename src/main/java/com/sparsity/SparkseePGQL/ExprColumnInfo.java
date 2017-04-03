package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.ir.QueryExpression;

import java.util.ArrayList;

import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.PROP_ACCESS;
import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.VARREF;

/**
 */
public class ExprColumnInfo extends ExpressionInfo {

    private static long seqColNumberGenerator = 0;

    public ExprColumnInfo(QueryExpression expr) {
        super(expr, true);
    }

    public ExprColumnInfo(QueryExpression expr, boolean parseIt) {
        super(expr, parseIt);
    }


    /**
     * If the expression is a simple access to an already existing column,
     * it will return the column number.
     * @param query
     * @return Returns the column number of -1 if it doesn't exists.
     */
    public int isAnExistingColumn( SQAQueryBuilder query ) {
        int colNum = -1;
        if (expression.getExpType() == PROP_ACCESS) {
            QueryExpression.PropertyAccess propExpr = (QueryExpression.PropertyAccess) expression;
            colNum = query.getAttributeColumnIndex( propExpr.getVariable().getName(), propExpr.getPropertyName() );
        }
        else if (expression.getExpType() == VARREF) {
            QueryExpression.VarRef varRef = (QueryExpression.VarRef) expression;
            String varName = varRef.getVariable().getName();
            if (query.containsVariable(varName)) {
                colNum = query.getVariableColumnIndex(varName);
            }
            else {
                colNum = query.getAliasPosition( varRef.getVariable().toString() );
            }
        }
        else {
            // Try to find an alias for the entire expression
            colNum = query.getAliasPosition( expression.toString() );
        }
        // TODO: Missing cases but with the aliases it may not be required
        return colNum;
    }


    public String getSQAExtendExpression( SQAQueryBuilder query ) {
        return getQuerySQAExpression(query, null);
    }


    public String GetUniqueName()
    {
        long num = seqColNumberGenerator++;
        return "Expression Column "+num;
    }


    /**
     * Adds all the columns required to calculate this expression, sets the
     * result column name alias to the given alias (if provided) and returns the
     * column position where the expression result can be found.
     * All the required variables must already exist in the query.
     * Any required variable attributes or types will be added in columns.
     * If the expression can be resolved just using an existing column, it will
     * be used. Otherwise a new column with the result of evaluating the expression
     * will be added.
     * @param query [in/out] The SQA query.
     * @param alias [in] An optional alias to the expression column.
     * @return Returns the position of the result query.
     */
    public int addExpressionColumn( SQAQueryBuilder query, String alias )
    {
        int numCol = -1;

        // Add the missing variable attributes and variable type columns
        this.addMissingExpressionColumns( query );

        // Check if the expression is a simple existing column access
        numCol = this.isAnExistingColumn( query );
        if ( numCol >= 0)
        {
            assert( numCol < query.getNumColumns() );
        }
        else {
            // A new column with the expression evaluation must be added
            String extendSQAExpression = this.getSQAExtendExpression(query);
            query.preppendQuery( "EXTEND( ");
            // TODO: The column type is required for the Sparksee Algebra but we may not be able to know it
            //       I'm setting the result as a STRING for now.
            query.appendQuery( ", [ STRING ]");
            query.appendQuery( ", [ STRING(" + extendSQAExpression + ") ] )");
            numCol = query.getNumColumns(); // Get the number before adding the column
            query.addExpressionColumn( this.GetUniqueName() );
        }

        assert (numCol > 0);
        if (alias != null) {
            // Set the new alias for this column
            query.setAlias( alias, numCol );
        }
        return numCol;
    }
}

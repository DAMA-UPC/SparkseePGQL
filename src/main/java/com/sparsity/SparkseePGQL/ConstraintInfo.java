package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.ir.QueryExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.*;

/**
 */
public class ConstraintInfo extends ExpressionInfo {
    final Logger logger = LoggerFactory.getLogger(ConstraintInfo.class);

    private ArrayList<ConnectionGroup> groups; // List of the groups that contain connections with variables used in the constraint

    public ConstraintInfo(QueryExpression expr) {
        super(expr, true);
        groups = new ArrayList<ConnectionGroup>();
    }

    @Override
    public String toString() {
        return "ConstraintInfo{" +
                "variables=" + variables +
                ", labels=" + labels +
                ", varRefs=" + varRefs +
                ", isBasicOp= "+ isABasicOperation() +
                '}';
    }

    /**
     * Checks if the expression can be translated as a single basic GRAPH::SELECT or
     * GRAPH::SCAN Sparksee algebra operation.
     *
     * @return Returns true if it's a simple operation.
     */
    public boolean isABasicOperation() {
        if ((variables.size() != 1) || (labels.size() > 1)) {
            return false;
        }
        if ((expression.getExpType() == HAS_LABEL) && (labels.size() == 1)) {
            QueryExpression.Function.HasLabel hasLabel = (QueryExpression.Function.HasLabel) expression;
            if ((hasLabel.getExp1().getExpType() == VARREF) && (hasLabel.getExp2().getExpType() == STRING)) {
                // GRAPH::SCAN('LABEL')
                return true;
            }
        } else if (labels.size() == 0) {
            ExprVariableInfo varInfo = getFirstVariable().getValue();
            assert (varInfo != null);
            if (varInfo.getAttributes().size() != 1) {
                // TODO: Handle more than one attribute in a simple operation
                return false;
            }

            // One var, One attribute, no labels
            if ((expression.getExpType() == EQUAL) ||
                    (expression.getExpType() == NOT_EQUAL) ||
                    (expression.getExpType() == GREATER) ||
                    (expression.getExpType() == GREATER_EQUAL) ||
                    (expression.getExpType() == LESS) ||
                    (expression.getExpType() == LESS_EQUAL)) {
                QueryExpression.BinaryExpression binExp = (QueryExpression.BinaryExpression) expression;
                if (binExp.getExp1().getExpType() == PROP_ACCESS) {
                    if (binExp.getExp2().getExpType() == STRING) {
                        QueryExpression.Constant.ConstString constString = ((QueryExpression.Constant.ConstString) binExp.getExp2());
                        //result.addLabel( constString.getValue() );
                        // GRAPH::SELECT( Atribut OP valor )
                        return true;
                    } else if (binExp.getExp2().getExpType() == INTEGER) {
                        QueryExpression.Constant.ConstInteger constInt = ((QueryExpression.Constant.ConstInteger) binExp.getExp2());
                        // GRAPH::SELECT( Atribut OP valor )
                        return true;
                    } else if (binExp.getExp2().getExpType() == DECIMAL) {
                        QueryExpression.Constant.ConstDecimal constDec = ((QueryExpression.Constant.ConstDecimal) binExp.getExp2());
                        // GRAPH::SELECT( Atribut OP valor )
                        return true;
                    } else if (binExp.getExp2().getExpType() == BOOLEAN) {
                        QueryExpression.Constant.ConstBoolean constBool = ((QueryExpression.Constant.ConstBoolean) binExp.getExp2());
                        // GRAPH::SELECT( Atribut = valor )
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }


    private String getSQAOPSymbol( QueryExpression.ExpressionType  exprType )
    {
        switch (exprType)
        {
            case EQUAL:
                return "=";
            case NOT_EQUAL:
                return "<>";
            case GREATER:
                return ">";
            case GREATER_EQUAL:
                return ">=";
            case LESS:
                return "<";
            case LESS_EQUAL:
                return "<=";
            default:
                return null;
        }
    }

    /**
     * Translates the expression as a basic Sparksee Query Albebra operation when possible.
     * @return Returns the SQA query string or null.
     */
    public SQAQueryBuilder getAsBasicSQAOperation() {
        if ((variables.size() != 1) || (labels.size() > 1)) {
            return null;
        }
        // In any valid case there will be only one output column that contains oids of the only variable used
        SQAQueryBuilder query = new SQAQueryBuilder();
        query.addVariableColumn( getFirstVariableName() );

        if ((expression.getExpType() == HAS_LABEL) && (labels.size() == 1)) {
            QueryExpression.Function.HasLabel hasLabel = (QueryExpression.Function.HasLabel) expression;
            if ((hasLabel.getExp1().getExpType() == VARREF) && (hasLabel.getExp2().getExpType() == STRING)) {
                // GRAPH::SCAN('LABEL')
                query.setQuery( "GRAPH::SCAN('"+((QueryExpression.Constant.ConstString) hasLabel.getExp2()).getValue()+ "')");
                return query;
            }
        } else if (labels.size() == 0) {
            ExprVariableInfo varInfo = getFirstVariable().getValue();
            assert (varInfo != null);
            if (varInfo.getAttributes().size() != 1) {
                // TODO: Handle more than one attribute in a simple operation
                return null;
            }

            // One var, One attribute, no labels
            if ((expression.getExpType() == EQUAL) ||
                    (expression.getExpType() == NOT_EQUAL) ||
                    (expression.getExpType() == GREATER) ||
                    (expression.getExpType() == GREATER_EQUAL) ||
                    (expression.getExpType() == LESS) ||
                    (expression.getExpType() == LESS_EQUAL)) {
                QueryExpression.BinaryExpression binExp = (QueryExpression.BinaryExpression) expression;
                if (binExp.getExp1().getExpType() == PROP_ACCESS) {
                    // In Sparksee we must use GLOBAL attributes because PGQL does not bind the attributes to a Type.
                    if (binExp.getExp2().getExpType() == STRING) {
                        QueryExpression.Constant.ConstString constString = ((QueryExpression.Constant.ConstString) binExp.getExp2());
                        query.setQuery( "GRAPH::SELECT( GLOBAL '" +
                                ((QueryExpression.PropertyAccess) binExp.getExp1()).getPropertyName() + "' " +
                                getSQAOPSymbol( expression.getExpType() ) + " '" +
                                constString.getValue() + "' )" );
                        return query;
                    } else if (binExp.getExp2().getExpType() == INTEGER) {
                        QueryExpression.Constant.ConstInteger constInt = ((QueryExpression.Constant.ConstInteger) binExp.getExp2());
                        query.setQuery( "GRAPH::SELECT( GLOBAL '" +
                                ((QueryExpression.PropertyAccess) binExp.getExp1()).getPropertyName() + "' " +
                                getSQAOPSymbol( expression.getExpType() ) + " " +
                                constInt.getValue() + " )" );
                        return query;
                    } else if (binExp.getExp2().getExpType() == DECIMAL) {
                        QueryExpression.Constant.ConstDecimal constDec = ((QueryExpression.Constant.ConstDecimal) binExp.getExp2());
                        query.setQuery(  "GRAPH::SELECT( GLOBAL '" +
                                ((QueryExpression.PropertyAccess) binExp.getExp1()).getPropertyName() + "' " +
                                getSQAOPSymbol( expression.getExpType() ) + " " +
                                constDec.getValue() + "F )" );
                        return query;
                    } else if (binExp.getExp2().getExpType() == BOOLEAN) {
                        QueryExpression.Constant.ConstBoolean constBool = ((QueryExpression.Constant.ConstBoolean) binExp.getExp2());
                        query.setQuery( "GRAPH::SELECT( GLOBAL '" +
                                ((QueryExpression.PropertyAccess) binExp.getExp1()).getPropertyName() + "' " +
                                getSQAOPSymbol( expression.getExpType() ) + " " +
                                (constBool.getValue()? "True" : "False") + " )" );
                        return query;
                    }
                }
                return null;
            }
        }
        return null;
    }


    /**
     * Builds a list of the groups with connections that use any of the variables used in the constraint
     * @param connGroups [in] A list of all the connection groups
     */
    public void linkGroups( ArrayList<ConnectionGroup> connGroups )
    {
       if (groups.size() > 0 ) {
           groups.clear();
       }

       for ( ConnectionGroup group : connGroups)
       {
           for (String varName : getVariablesKeySet())
           {
               if (group.containsVariable(varName))
               {
                   groups.add(group);
                   break;
               }
           }
       }
    }

    public int getNumLinkedGroups() {
        return groups.size();
    }

    public ConnectionGroup getFirstLinkedGroup() {
        if ((groups != null) && (groups.size()>=1))
        {
            return groups.get(0);
        }
        return null;
    }

    public ArrayList<ConnectionGroup> getGroups() {
        return groups;
    }


    public String getQueryConstraintExpression( SQAQueryBuilder query, SQAQueryBuilder query2 ) {
        // All the variables, attributes and labels must have already been added tot the query!
        // Apply the constraint
        logger.debug("Getting the constraint: " + toString());
        return getQuerySQAExpression(query, query2);
    }

    /**
     * Informs the constraint that the group g2 had been merged into g1.
     * So if g2 is in the groups of this constraint it must be replaced with g1.
     */
    public void mergedGroups( ConnectionGroup g1, ConnectionGroup g2 ) {
        if (groups.contains(g2))
        {
            groups.remove(g2);
        }
        if (!groups.contains(g1))
        {
            groups.add(g1);
        }
    }


    /**
     * Applies the given constraint to the query.
     * The query must already contain all the variables required so the constraint only needs one
     * query and it's only one or more selections.
     * But the constaint may require to obtain first some attributes or label ids.
     * @param query [in/out] The query to update.
     */
    public void applyConstraint(SQAQueryBuilder query)
    {
        // Get the information that the constraint will need
        this.addMissingExpressionColumns(query);

        // Apply the constraint
        query.preppendQuery("SELECT( ");
        query.appendQuery(", "+this.getQueryConstraintExpression(query, null)+" )");
    }



    /**
     * Applies the given JOIN constraint to the query.
     * Each query must already contain all the variables required so the constraint only needs
     * to join the two given queries.
     * But the constaint may require to obtain first some attributes or label ids for each query.
     * @param query [in/out] The query to update.
     * @param query2 [in/out] The second query of the join.
     */
    protected void applyJOINConstraint( SQAQueryBuilder query, SQAQueryBuilder query2)
    {
        assert(this.getNumLinkedGroups() == 2);
        // Get the information that the constraint will need from each query
        this.addMissingExpressionColumns(query);
        this.addMissingExpressionColumns(query2);

        // Apply the constraint
        query.preppendQuery("JOIN( ");
        query.appendQuery(", " + query2.getQuery()+", "+
                this.getQueryConstraintExpression(query, query2)+", {'type'='Inner'} )");
        // Add to the query all the joined columns
        query.addColumnsData(query2);
    }
    
    
}

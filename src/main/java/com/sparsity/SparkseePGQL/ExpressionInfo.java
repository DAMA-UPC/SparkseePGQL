package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.ir.ExpAsVar;
import oracle.pgql.lang.ir.QueryExpression;
import oracle.pgql.lang.ir.QueryExpressionVisitor;
import oracle.pgql.lang.ir.QueryVariable;

import java.text.DecimalFormat;
import java.util.*;

import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.EDGE_LABEL;
import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.STRING;
import static oracle.pgql.lang.ir.QueryExpression.ExpressionType.VARREF;

/**
 */
public class ExpressionInfo {
    protected final HashMap<String, ExprVariableInfo> variables;
    protected final HashSet<String> labels; // Labels que s'utilitzen (noms de tipus)
    protected final HashSet<String> varRefs; // Referencies directes a columnes que poden ser
                                             // variables o be Alies (noms donats explicitament a expressions)
    protected boolean hasAggregateOps; // True if the query is using aggregate operations.
    protected final QueryExpression expression;
    // Aixo es per poder mirar facilment quins tenen ja totes les variables necessaries
    protected int numPendingVars; // Number of variables used in the constraint and not yet calculated

    
    protected class NormalVisitor implements  QueryExpressionVisitor
    {
        protected ExpressionInfo parent;
        
        NormalVisitor( ExpressionInfo parent ) {
            this.parent = parent;
        }
        
        @Override
        public void visit(QueryExpression.VarRef varRef) {
            // Aixo passa quan s'accedeix a una variable directament o a l'alias d'una
            // columna calculada a la que s'ha donat expliciatament un nom.
            parent.addVarRef(varRef.getVariable().getName());
        }

        @Override
        public void visit(QueryExpression.PropertyAccess propAccess) {
            ExprVariableInfo info = parent.addVariable(propAccess.getVariable().getName());
            info.addAttribute(propAccess.getPropertyName());
        }

        @Override
        public void visit(QueryExpression.Constant.ConstInteger constInteger) {
        }

        @Override
        public void visit(QueryExpression.Constant.ConstDecimal constDecimal) {
        }

        @Override
        public void visit(QueryExpression.Constant.ConstString constString) {
            //parent.append(" ConstString: "+constString.getValue());
        }

        @Override
        public void visit(QueryExpression.Constant.ConstBoolean constBoolean) {
        }

        @Override
        public void visit(QueryExpression.ConstNull constantNull) {
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.Sub sub) {
            sub.getExp1().accept(this);
            sub.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.Add add) {
            add.getExp1().accept(this);
            add.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.Mul mul) {
            mul.getExp1().accept(this);
            mul.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.Div div) {
            div.getExp1().accept(this);
            div.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.Mod mod) {
            mod.getExp1().accept(this);
            mod.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.ArithmeticExpression.UMin uMin) {
            uMin.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.LogicalExpression.And and) {
            and.getExp1().accept(this);
            and.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.LogicalExpression.Or or) {
            or.getExp1().accept(this);
            or.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.LogicalExpression.Not not) {
            not.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.Equal equal) {
            equal.getExp1().accept(this);
            //parent.append(" = ");
            equal.getExp2().accept(this);

            if ((equal.getExp1().getExpType() == EDGE_LABEL) &&
                    (equal.getExp2().getExpType() == STRING)) {
                QueryExpression.Constant.ConstString constString = ((QueryExpression.Constant.ConstString) equal.getExp2());
                parent.addLabel(constString.getValue());
            }
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.NotEqual notEqual) {
            notEqual.getExp1().accept(this);
            notEqual.getExp2().accept(this);

            if ((notEqual.getExp1().getExpType() == EDGE_LABEL) &&
                    (notEqual.getExp2().getExpType() == STRING)) {
                QueryExpression.Constant.ConstString constString = ((QueryExpression.Constant.ConstString) notEqual.getExp2());
                parent.addLabel(constString.getValue());
            }
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.Greater greater) {
            greater.getExp1().accept(this);
            greater.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.GreaterEqual greaterEqual) {
            greaterEqual.getExp1().accept(this);
            greaterEqual.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.Less less) {
            less.getExp1().accept(this);
            less.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.RelationalExpression.LessEqual lessEqual) {
            lessEqual.getExp1().accept(this);
            lessEqual.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrCount aggrCount) {
            hasAggregateOps = true;
            aggrCount.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrMin aggrMin) {
            hasAggregateOps = true;
            aggrMin.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrMax aggrMax) {
            hasAggregateOps = true;
            aggrMax.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrSum aggrSum) {
            hasAggregateOps = true;
            aggrSum.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrAvg aggrAvg) {
            hasAggregateOps = true;
            aggrAvg.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Star star) {
        }

        @Override
        public void visit(QueryExpression.Function.Regex regex) {
            regex.getExp1().accept(this);
            regex.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.Function.Id id) {
            id.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Function.HasProp hasProp) {
            // TODO?
            hasProp.getExp1().accept(this);
            hasProp.getExp2().accept(this);
        }

        @Override
        public void visit(QueryExpression.Function.HasLabel hasLabel) {
            // TODO?
            hasLabel.getExp1().accept(this);
            assert (hasLabel.getExp1().getExpType() == VARREF);
            QueryExpression.VarRef varRef = ((QueryExpression.VarRef) hasLabel.getExp1());
            ExprVariableInfo info = parent.addVariable(varRef.getVariable().getName());
            info.setLabelChecked();
//                parent.append(" hasLabel ");
            hasLabel.getExp2().accept(this);
            assert (hasLabel.getExp2().getExpType() == STRING);
            QueryExpression.Constant.ConstString constString = ((QueryExpression.Constant.ConstString) hasLabel.getExp2());
            parent.addLabel(constString.getValue());
        }

        @Override
        public void visit(QueryExpression.Function.VertexLabels vertexLabels) {
            //vertexLabels.getExp().accept(this);
//                parent.append("VertexLabels: "+vertexLabels.toString());
            // TODO: May need the same as the edgeLabel
        }

        @Override
        public void visit(QueryExpression.Function.InDegree inDegree) {
            inDegree.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Function.OutDegree outDegree) {
            outDegree.getExp().accept(this);
        }

        @Override
        public void visit(QueryExpression.Function.EdgeLabel edgeLabel) {
            assert (edgeLabel.getExpType() == EDGE_LABEL);
            assert (edgeLabel.getExp().getExpType() == VARREF);
            QueryExpression.VarRef varRef = ((QueryExpression.VarRef) edgeLabel.getExp());
            ExprVariableInfo info = parent.addVariable(varRef.getVariable().getName());
            info.setLabelChecked();
        }
    }




    public ExpressionInfo(QueryExpression expr, boolean parseIt) {
        variables = new HashMap<String, ExprVariableInfo>();
        labels = new HashSet<String>();
        varRefs = new HashSet<String>();
        expression = expr;
        numPendingVars = 0;
        hasAggregateOps = false;
        if (parseIt) {
            parseQueryExpression();
        }
    }




    public ExprVariableInfo addVariable(String name) {
        if (!variables.containsKey(name)) {
            ExprVariableInfo info = new ExprVariableInfo();
            variables.put(name, info);
            ++numPendingVars;
            return info;
        } else {
            return variables.get(name);
        }

    }

    public void addLabel(String name) {
        if (!labels.contains(name)) {
            labels.add(name);
        }
    }

    public void addVarRef(String name) {
        if (!varRefs.contains(name)) {
            varRefs.add(name);
        }
    }

    public Set<String> getVariablesKeySet() {
        return variables.keySet();
    }

    public Map.Entry<String, ExprVariableInfo> getFirstVariable() {
        for (Map.Entry<String, ExprVariableInfo> varEntry : variables.entrySet()) {
            return varEntry;
        }
        return null;
    }

    public String getFirstVariableName() {
        Map.Entry<String, ExprVariableInfo> varEntry = getFirstVariable();
        if (varEntry != null) {
            return varEntry.getKey();
        }
        return null;
    }


    public ExprVariableInfo getVariableInfo(String varName ) {
        return variables.get(varName);
    }


    public void parseQueryExpression() {
        expression.accept(new NormalVisitor(this));
    }

    @Override
    public String toString() {
        return "ExpressionInfo{" +
                "variables=" + variables +
                ", labels=" + labels +
                ", varRefs=" + varRefs +
                '}';
    }


    public void updatePendingVariables(ArrayList<String> vars) {
        for( String varName : vars) {
            if ( variables.containsKey(varName) ) {
                assert(numPendingVars > 0);
                --numPendingVars;
            }
        }
    }

    public int getNumPendingVariables() { return numPendingVars; }

    public boolean hasAggregates() { return hasAggregateOps; }

    /**
     * Builds the Sparksee Algebra Expression to apply as a constraint or a calculated column.
     * All the required variables, attributes and labels must have already been added tot each query!
     * If the second query is null, then the expression can only use variables in the first query
     * (of a single group of connections or already joined groups).
     * If a second query is provided, then this constraint expression may join two queries (two
     * connection group queries into one).
     * @param query [in/out] The query where the expression will be applied.
     * @param query [in] The optional second query that may be used to join with the first one.
     * @return Returns the Sparksee Query Algebra expression
     */
    public String getQuerySQAExpression( SQAQueryBuilder query, SQAQueryBuilder query2 )
    {
        // All the variables, attributes and labels must have already been added tot the query!
        StringBuilder sqaExpr = new StringBuilder();
        expression.accept(new QueryExpressionVisitor() {

            @Override
            public void visit(QueryExpression.VarRef varRef) {
                sqaExpr.append( sqaVarRefExpression( query, query2, varRef.getVariable().getName() ) );
            }


            @Override
            public void visit(QueryExpression.PropertyAccess propAccess) {
                sqaExpr.append( sqaAttrExpression(query, query2, propAccess.getVariable().getName(),
                        propAccess.getPropertyName()) );
            }

            @Override
            public void visit(QueryExpression.Constant.ConstInteger constInteger) {
                sqaExpr.append(constInteger.getValue());
            }

            @Override
            public void visit(QueryExpression.Constant.ConstDecimal constDecimal) {
                DecimalFormat formatter = new DecimalFormat("#0.0#");
                sqaExpr.append(formatter.format(constDecimal.getValue()));
            }

            @Override
            public void visit(QueryExpression.Constant.ConstString constString) {
                sqaExpr.append("'"+constString.getValue()+"'");
            }

            @Override
            public void visit(QueryExpression.Constant.ConstBoolean constBoolean) {
                sqaExpr.append(constBoolean.getValue()? "TRUE" : "FALSE");
            }

            @Override
            public void visit(QueryExpression.ConstNull constantNull) {
                // TODO: Can't just translate to null because if the expression
                // is something like varRef = NULL or varRef != NULL, it should
                // be translated as IS_NULL(varRef) or NOT(IS_NULL(varRef).
                assert(false);
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.Sub sub) {
                sqaExpr.append("( ");
                sub.getExp1().accept(this);
                sqaExpr.append(" ) - ( ");
                sub.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.Add add) {
                sqaExpr.append("( ");
                add.getExp1().accept(this);
                sqaExpr.append(" ) + ( ");
                add.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.Mul mul) {
                sqaExpr.append("( ");
                mul.getExp1().accept(this);
                sqaExpr.append(" ) * ( ");
                mul.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.Div div) {
                sqaExpr.append("( ");
                div.getExp1().accept(this);
                sqaExpr.append(" ) / ( ");
                div.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.Mod mod) {
                // TODO: Add the MOD operator to the Sparksee Algebra.
                assert(false);
                sqaExpr.append("( ");
                mod.getExp1().accept(this);
                sqaExpr.append(" TODO MOD OPERATOR ");
                mod.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.ArithmeticExpression.UMin uMin) {
                sqaExpr.append( "-( ");
                uMin.getExp().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.LogicalExpression.And and) {
                sqaExpr.append("( ");
                and.getExp1().accept(this);
                sqaExpr.append(" ) AND ( ");
                and.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.LogicalExpression.Or or) {
                sqaExpr.append("( ");
                or.getExp1().accept(this);
                sqaExpr.append(" ) OR ( ");
                or.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.LogicalExpression.Not not) {
                sqaExpr.append("NOT ( ");
                not.getExp().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.Equal equal) {
                sqaExpr.append("( ");
                equal.getExp1().accept(this);
                sqaExpr.append(" ) = ( ");
                equal.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.NotEqual notEqual) {
                sqaExpr.append("( ");
                notEqual.getExp1().accept(this);
                sqaExpr.append(" ) <> ( ");
                notEqual.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.Greater greater) {
                sqaExpr.append("( ");
                greater.getExp1().accept(this);
                sqaExpr.append(" ) > ( ");
                greater.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.GreaterEqual greaterEqual) {
                sqaExpr.append("( ");
                greaterEqual.getExp1().accept(this);
                sqaExpr.append(" ) >= ( ");
                greaterEqual.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.Less less) {
                sqaExpr.append("( ");
                less.getExp1().accept(this);
                sqaExpr.append(" ) < ( ");
                less.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.LessEqual lessEqual) {
                sqaExpr.append("( ");
                lessEqual.getExp1().accept(this);
                sqaExpr.append(" ) <= ( ");
                lessEqual.getExp2().accept(this);
                sqaExpr.append(" )");
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrCount aggrCount) {
                // An aggregate operation at this point must have already been computed
                sqaExpr.append( sqaVarRefExpression( query, query2, "COUNT("+aggrCount.getExp().toString()+")" ));
                //aggrCount.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrMin aggrMin) {
                // An aggregate operation at this point must have already been computed
                sqaExpr.append( sqaVarRefExpression( query, query2, "MIN("+aggrMin.getExp().toString()+")" ));
                //aggrMin.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrMax aggrMax) {
                // An aggregate operation at this point must have already been computed
                sqaExpr.append( sqaVarRefExpression( query, query2, "MAX("+aggrMax.getExp().toString()+")" ));
                //aggrMax.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrSum aggrSum) {
                // An aggregate operation at this point must have already been computed
                sqaExpr.append( sqaVarRefExpression( query, query2, "SUM("+aggrSum.getExp().toString()+")" ));
                //aggrSum.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrAvg aggrAvg) {
                // An aggregate operation at this point must have already been computed
                sqaExpr.append( sqaVarRefExpression( query, query2, "AVG("+aggrAvg.getExp().toString()+")" ));
                //aggrAvg.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Star star) {
                assert(false); // Crec que aixo no te sentit en un constraint que nomes afecta a un grup
            }

            @Override
            public void visit(QueryExpression.Function.Regex regex) {
                // TODO!
                regex.getExp1().accept(this);
                sqaExpr.append(" regex ");
                regex.getExp2().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.Id id) {
                // TODO!
                sqaExpr.append("ID: ");
                id.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.HasProp hasProp) {
                // TODO!
                assert(false);
                //assert ((hasProp.getExp1().getExpType() == VARREF) && (hasProp.getExp2().getExpType() == STRING));
                hasProp.getExp1().accept(this);
                sqaExpr.append(" TODO_hasProp: ");
                hasProp.getExp2().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.HasLabel hasLabel) {
                assert ((hasLabel.getExp1().getExpType() == VARREF) && (hasLabel.getExp2().getExpType() == STRING));
                sqaExpr.append(
                        sqaLabelExpression(query, query2, ((QueryExpression.VarRef)hasLabel.getExp1()).getVariable().getName())+
                                " = "+
                                sqaTypeExpression(((QueryExpression.Constant.ConstString) hasLabel.getExp2()).getValue()) );
            }

            @Override
            public void visit(QueryExpression.Function.VertexLabels vertexLabels) {
                // TODO!
                assert(false);
                //vertexLabels.getExp().accept(this);
                sqaExpr.append("TODOVertexLabels: "+vertexLabels.toString());
            }

            @Override
            public void visit(QueryExpression.Function.InDegree inDegree) {
                // TODO!
                assert(false);
                sqaExpr.append("TODO inDegree FUNCTION");
                inDegree.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.OutDegree outDegree) {
                // TODO!
                assert(false);
                sqaExpr.append("TODO outDegree FUNCTION");
                outDegree.getExp().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.EdgeLabel edgeLabel) {
                assert(edgeLabel.getExp().getExpType() == VARREF);
                sqaExpr.append( sqaLabelExpression(query, query2, ((QueryExpression.VarRef)edgeLabel.getExp()).getVariable().getName()) );
            }
        });
        return sqaExpr.toString();
    }


    protected String sqaTypeExpression(String typeName) {
        return "'"+typeName+"'";
    }

    protected String sqaVarExpression(SQAQueryBuilder query, SQAQueryBuilder query2, String varName) {
        if (query.containsVariable(varName)) {
            return "%" + query.getVariableColumnIndex(varName);
        }
        else {
            assert((query2 != null) && (query2.containsVariable(varName)));
            return "%" + (query.getNumColumns() + query2.getVariableColumnIndex(varName));
        }
    }

    protected String sqaAttrExpression(SQAQueryBuilder query, SQAQueryBuilder query2, String varName, String attrName) {
        if (query.containsVariable(varName)) {
            return "%"+query.getAttributeColumnIndex(varName, attrName);
        }
        else {
            assert((query2 != null) && (query2.containsVariable(varName)));
            return "%" + (query.getNumColumns() + query2.getAttributeColumnIndex(varName, attrName));
        }
    }

    protected String sqaLabelExpression(SQAQueryBuilder query, SQAQueryBuilder query2, String varName) {
        if (query.containsVariable(varName)) {
            return "%"+query.getLabelColumnIndex(varName);
        }
        else {
            assert((query2 != null) && (query2.containsVariable(varName)));
            return "%" + (query.getNumColumns() + query2.getLabelColumnIndex(varName));
        }
    }


    protected String sqaVarRefExpression(SQAQueryBuilder query, SQAQueryBuilder query2, String varName) {
        if (query.containsVariable(varName)) {
            return "%" + query.getVariableColumnIndex(varName);
        }
        else if ( (query2 != null) && (query2.containsVariable(varName)) ) {
            return "%" + (query.getNumColumns() + query2.getVariableColumnIndex(varName));
        }
        else {
            // Could be an alias to a calculated column instead of a variable (node/edge)
            // Or an aggregate operation
            assert(query2 == null); // ALIAS are not available in constraints, so it should not be used to join
            int colIndex = query.getAliasPosition( varName );
            if (colIndex >= 0) {
                return "%"+colIndex;
            }
            else {
                colIndex = query.getAggregateColumnIndex( varName );
                if (colIndex >= 0) {
                    return "%"+colIndex;
                }
                else {
                    assert (false);
                    return null;
                }
            }
        }
    }



    /**
     * Adds the given attributes to the columns calculated by the query,
     * unless the attributes are already calculated.
     * @param query [in/out] The query to update.
     * @param varName [in] The variable name.
     * @param attrs [in] The set of attribute names.
     */
    public static void addSQAAttribute( SQAQueryBuilder query, String varName, Set<String> attrs) {
        boolean attrsAdded = false;
        for (String attrName : attrs) {
            if (!query.containsAttribute(varName, attrName)) {
                if (!attrsAdded)
                {
                    query.preppendQuery("GRAPH::GET(");
                    query.appendQuery(", "+query.getVariableColumnIndex(varName)+ ", [");
                    query.appendQuery(" GLOBAL '"+attrName+"'");
                    // TODO: We only can use Sparksee GLOBAL attributes with PGQL
                    attrsAdded = true;
                }
                else {
                    query.appendQuery(", GLOBAL '"+attrName+"'");
                }
                query.addAttributeColumn(varName, attrName);
            }
        }
        if (attrsAdded) {
            query.appendQuery(" ])");
        }
    }




    /**
     * Adds the columns required for the expression that are still missing from the query
     * but are related to variables present in the query.
     * It can add attribute columns, get_type columns, ...
     * @param query [in/out] The query to update
     */
    protected void addMissingExpressionColumns(SQAQueryBuilder query) {
        // Get the information that the constraint will need
        for (String varName : this.getVariablesKeySet()) {
            // This method may be used with expressions that join several queries,
            // so not all the variables may be present in the given query.
            if ( query.containsVariable( varName )) {
                ExprVariableInfo exprVarInfo = this.getVariableInfo(varName);

                // Get all the attributes missing
                ExpressionInfo.addSQAAttribute(query, varName, exprVarInfo.getAttributes());

                // Get the labels of the variables where the label is checked
                if (exprVarInfo.isLabelChecked() && !query.containsLabel(varName)) {
                    query.preppendQuery("GRAPH::GET_TYPE( ");
                    query.appendQuery(", " + query.getVariableColumnIndex(varName) + " )");
                    query.addLabelColumn(varName);
                }

                // TODO: The constraints may use the inDegree and outDegree functions. So we should
                // know for which variables this functions are needed and add here the columns
                // of this inDegree/outDegree functions for each variable. Then this columns could
                // be used when the constraint expressions are applied.
                // The InDegree and OutDegree funcions must be added to the Sparksee Algebra too.
            }
        }
    }
    
    
}

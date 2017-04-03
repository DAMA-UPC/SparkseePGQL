package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.ir.QueryExpression;
import oracle.pgql.lang.ir.QueryExpressionVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 */
public class PostGroupExprColumnInfo extends ExprColumnInfo {

    final Logger logger = LoggerFactory.getLogger(PostGroupExprColumnInfo.class);

    public PostGroupExprColumnInfo(QueryExpression expr) {
        super(expr, false);
        // The Expression is not parsed here because it must be done after grouping and ignoring
        // the requirements inside the aggregate functions.
    }

    protected class IgnoreAggregatesVisitor extends ExpressionInfo.NormalVisitor {

        IgnoreAggregatesVisitor(PostGroupExprColumnInfo parent) {
            super(parent);
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrCount aggrCount) {
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrMin aggrMin) {
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrMax aggrMax) {
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrSum aggrSum) {
        }

        @Override
        public void visit(QueryExpression.Aggregation.AggrAvg aggrAvg) {
        }
    }

    @Override
    public void parseQueryExpression() {
        expression.accept(new IgnoreAggregatesVisitor(this));
    }

    /**
     * Creates a new AggregateInfo for the given aggregate type and expression, after
     * having added to the given query all the columns required to calculate the
     * aggregate function expression.
     * @param query [in/out] The query to update
     * @param type [in] The aggregate kind
     * @param expr [in] The aggregate function input data expression
     * @return Returns a new AggregateInfo
     */
    protected static AggregateInfo addAggregateInfo(SQAQueryBuilder query, AggregateInfo.AggregateType type, QueryExpression expr) {
        int numCol = -1;
        if (expr.getExpType() == QueryExpression.ExpressionType.STAR) {
            assert (type == AggregateInfo.AggregateType.COUNT_AGGR);
            // La forma d'indicar que no depen de cap columna es el -1
        }
        else {
            // The ExprColumnInfo class will parse the expression requirements
            ExprColumnInfo eInfo = new ExprColumnInfo(expr);
            // This method will also add all the required columns to calculate the expression
            numCol = eInfo.addExpressionColumn(query, null);
        }
        AggregateInfo aggInfo = new AggregateInfo(expr.toString(), type, numCol);
        return aggInfo;
    }


    /**
     * Parses the expression adding to the query all the columns needed to calculate the inner subexpressions
     * used in aggregate operations and updates the given argument with the aggregate operations information.
     * The columns needed outside aggregate operations are ignored.
     * @param query [in/out] The query to update
     * @param aggregateInfos [in/out] The aggregates operations to update
     */
    public void extractAggregatesInfo(SQAQueryBuilder query, ArrayList<AggregateInfo> aggregateInfos)
    {
        ExprColumnInfo parent = this;

        expression.accept(new QueryExpressionVisitor() {

            @Override
            public void visit(QueryExpression.Aggregation.AggrCount aggrCount) {
                hasAggregateOps = true;
                aggregateInfos.add( addAggregateInfo(query, AggregateInfo.AggregateType.COUNT_AGGR, aggrCount.getExp()) );
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrMin aggrMin) {
                hasAggregateOps = true;
                aggregateInfos.add( addAggregateInfo(query, AggregateInfo.AggregateType.MIN_AGGR, aggrMin.getExp()) );
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrMax aggrMax) {
                hasAggregateOps = true;
                aggregateInfos.add( addAggregateInfo(query, AggregateInfo.AggregateType.MAX_AGGR, aggrMax.getExp()) );
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrSum aggrSum) {
                hasAggregateOps = true;
                aggregateInfos.add( addAggregateInfo(query, AggregateInfo.AggregateType.SUM_AGGR, aggrSum.getExp()) );
            }

            @Override
            public void visit(QueryExpression.Aggregation.AggrAvg aggrAvg) {
                hasAggregateOps = true;
                aggregateInfos.add( addAggregateInfo(query, AggregateInfo.AggregateType.AVG_AGGR, aggrAvg.getExp()) );
            }

            @Override
            public void visit(QueryExpression.VarRef varRef) {
                // Aixo s'utilitza si l'aggregat (o qualsevol altra referencia) ja s'ha fet servir anteriorment.
                // O com a minim passa amb COUNT(*).
                // No tinc clar com s'ha de solucionar.
                logger.debug("S'utilitza un varRef en una PostGroup expression \""+varRef.toString()+ "\" amb id:"+varRef.hashCode());
                // Aqui segurament no cal fer res perque si esta repetit es que ja el tenim guardat
                // Pero potser cal preveure alguna cosa per quan s'hagi d'utilitzar al
                // fer la projeccio o l'ordenacio perque l'id que tenim ara (el hash) no sembla que
                // serveixi de res perque el de la referencia es diferent del de l'expressio original.
                // Un altre problema es que es pot haver arribat aqui al utilitzar coses com alies i
                // no nomes agregats, per tant, no dec poder fer res aqui sino que cal haver guardat
                // informacio per identificar l'aggregat quan s'ha produit per primer cop.

            }

            // The other methods just traverse the expression

            @Override
            public void visit(QueryExpression.Star star) {
            }

            @Override
            public void visit(QueryExpression.PropertyAccess propAccess) {
            }

            @Override
            public void visit(QueryExpression.Constant.ConstInteger constInteger) {
            }

            @Override
            public void visit(QueryExpression.Constant.ConstDecimal constDecimal) {
            }

            @Override
            public void visit(QueryExpression.Constant.ConstString constString) {
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
                equal.getExp2().accept(this);
            }

            @Override
            public void visit(QueryExpression.RelationalExpression.NotEqual notEqual) {
                notEqual.getExp1().accept(this);
                notEqual.getExp2().accept(this);
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
                hasProp.getExp1().accept(this);
                hasProp.getExp2().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.HasLabel hasLabel) {
                hasLabel.getExp1().accept(this);
                hasLabel.getExp2().accept(this);
            }

            @Override
            public void visit(QueryExpression.Function.VertexLabels vertexLabels) {
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
            }
        });
    }



}

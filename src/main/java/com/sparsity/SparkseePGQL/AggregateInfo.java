package com.sparsity.SparkseePGQL;

/**
 */
public class AggregateInfo {
    public enum AggregateType { COUNT_AGGR, MIN_AGGR, MAX_AGGR, SUM_AGGR, AVG_AGGR }
    private String subexpr; // Aggregate operation data (the expression string)
    private int srcColIndex; // Column index in the query that contains the operation input data or -1 if it's "*"
    private  AggregateType type;

    public AggregateInfo( String subexpr, AggregateType type, int srcColIndex ) {
       this.subexpr = subexpr;
       this.type = type;
       this.srcColIndex = srcColIndex;
       assert((type == AggregateType.COUNT_AGGR) || (srcColIndex>=0));
    }

    public String getSubexpr() {
        return subexpr;
    }

    public AggregateType getType() {
        return type;
    }

    public static String AggregateTypeName(AggregateType type )
    {
        switch (type) {
            case COUNT_AGGR:
                return("COUNT");
            case MIN_AGGR:
                return ("MIN");
            case MAX_AGGR:
                return ("MAX");
            case AVG_AGGR:
                return ("AVG");
            case SUM_AGGR:
                return ("SUM");
        }
        assert(false);
        return null;
    }

    public String getGroupSQAExpression() {
        switch (type) {
            case COUNT_AGGR:
                if (srcColIndex >=0) {
                    return("COUNT("+srcColIndex+")");
                }
                else {
                    return ("COUNT(ALL)");
                }
            case MIN_AGGR:
                return ("MIN("+srcColIndex+")");
            case MAX_AGGR:
                return ("MAX("+srcColIndex+")");
            case AVG_AGGR:
                return ("AVG("+srcColIndex+")");
            case SUM_AGGR:
                return ("SUM("+srcColIndex+")");
        }
        assert(false);
        return null;
    }
}

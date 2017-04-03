package com.sparsity.SparkseePGQL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class SQAQueryBuilder {
    final Logger logger = LoggerFactory.getLogger(SQAQueryBuilder.class);

    public enum ColumnType {
        VAR_COLUMN, ATTR_COLUMN, LABEL_COLUMN, OTHER_COLUMN, EXPRESSION_COLUMN, AGGREGATE_COLUMN
    }

    public class ColumnData {
        private String name; // Main name (varName in most cases)
        private String secondName; // The other relevant name (attrName probably)
        private int position;
        private ColumnType cType;
        // datatype?

        public ColumnData(String name, String secondName, int position, ColumnType cType) {
            this.name = name;
            this.secondName = secondName;
            this.position = position;
            this.cType = cType;
        }
    }

    private ArrayList<ColumnData> columns;
    private HashMap<String, ColumnData> nameToColumn;
    private HashMap<String, ColumnData> aliasToColumn;

    private StringBuilder query;

    public SQAQueryBuilder()
    {
        this.columns = new ArrayList<ColumnData>();
        this.nameToColumn = new HashMap<String, ColumnData>();
        this.aliasToColumn = new HashMap<String, ColumnData>();
        this.query = new StringBuilder();
    }

    public String varNameToKey(String name) {
        return "VAR//"+name;
    }
    public String attrNameToKey(String varName, String attrName) {
        return "ATTR//"+attrName+"//VAR//"+varName;
    }
    public String labelNameToKey(String name) {
        //The name string is the variable from which it's type is stored in the column
        return "LAB//"+name;
    }
    public String otherNameToKey(String name) {
        return "OTHER//"+name;
    }
    public String exprNameToKey(String name) {
        return "EXPR//"+name;
    }
    public String aggrNameToKey( String subExpr, String opName) {
        return "AGGR//"+opName+"("+subExpr+")";
    }

    public void addVariableColumn(String name ) {
        String str = varNameToKey(name);
        ColumnData column = new ColumnData( name, "", columns.size(), ColumnType.VAR_COLUMN );
        columns.add( column );
        nameToColumn.put( str, column);
    }

    public int getVariableColumnIndex( String name ) {
        return nameToColumn.get(varNameToKey(name)).position;
    }

    public void addAttributeColumn( String varName, String attrName ) {
        String str = attrNameToKey(varName, attrName);
        ColumnData column = new ColumnData( varName, attrName, columns.size(), ColumnType.ATTR_COLUMN );
        columns.add( column );
        nameToColumn.put( str, column);
    }

    public void addLabelColumn( String varName ) {
        String str = labelNameToKey(varName);
        ColumnData column = new ColumnData( varName, "", columns.size(), ColumnType.LABEL_COLUMN );
        columns.add( column );
        nameToColumn.put(str, column);
    }

    public void addOtherColumn( String name )
    {
        String str = otherNameToKey(name);
        ColumnData column = new ColumnData( name, "", columns.size(), ColumnType.OTHER_COLUMN );
        columns.add( column );
        nameToColumn.put(str, column);
    }

    public void addExpressionColumn( String name )
    {
        String str = exprNameToKey(name);
        ColumnData column = new ColumnData( name, "", columns.size(), ColumnType.EXPRESSION_COLUMN );
        columns.add( column );
        nameToColumn.put(str, column);
    }

    public void addAggregateColumn( AggregateInfo.AggregateType type, String subExpr )
    {
        String opName = AggregateInfo.AggregateTypeName(type);
        String str = aggrNameToKey(subExpr, opName);
        ColumnData column = new ColumnData( subExpr, opName, columns.size(), ColumnType.AGGREGATE_COLUMN );
        columns.add( column );
        nameToColumn.put(str, column);
        // Add an alias to the entire aggregate expression
        setAlias( opName+"("+subExpr+")", column.position );
    }

    void setQuery( String str )
    {
        query.setLength(0);
        query.append( str );
    }


    void appendQuery(String str)
    {
        query.append(str);
    }

    void preppendQuery(String str) {
        query.insert(0, str);
    }

    String getQuery()
    {
        return query.toString();
    }

    Boolean containsVariable( String name )
    {
        return nameToColumn.containsKey(varNameToKey(name));
    }

    Boolean containsAttribute( String varName, String attrName )
    {
        return nameToColumn.containsKey(attrNameToKey(varName, attrName));
    }

    public int getAttributeColumnIndex( String varName, String attrName ) {
        return nameToColumn.get(attrNameToKey(varName, attrName)).position;
    }

    Boolean containsLabel( String varName )
    {
        return nameToColumn.containsKey(labelNameToKey(varName));
    }

    public int getLabelColumnIndex( String varName ) {
        return nameToColumn.get(labelNameToKey(varName)).position;
    }

    int getNumColumns() { return columns.size(); }


    /**
     * Add to this query all the columns data of the given query.
     * This operation should be used after both queries are joined.
     * @param query [in] The query with the columns to be added.
     */
    public void addColumnsData( SQAQueryBuilder query ) {
        int initialSize = columns.size();
       for (ColumnData colData : query.columns) {
           int newColPosition = colData.position+initialSize;
           assert(newColPosition == columns.size());
           switch (colData.cType) {
               case VAR_COLUMN:
                  addVariableColumn(colData.name);
                  break;
               case ATTR_COLUMN:
                   addAttributeColumn(colData.name, colData.secondName);
                   break;
               case LABEL_COLUMN:
                   addLabelColumn(colData.name);
                   break;
               case OTHER_COLUMN:
                   addOtherColumn(colData.name);
                   break;
               default:
                   // It will never be used at the stages where other column types are possible
                   assert(false);
           }
       }
    }


    /**
     *
     * @return Returns an array with the names of all the VARIABLES in the columns of this query.
     */
    public ArrayList<String> getVariables()
    {
        ArrayList<String> varNames = new ArrayList<String>();
        for (ColumnData colData : columns) {
           if (colData.cType == ColumnType.VAR_COLUMN) {
               varNames.add( colData.name );
           }
        }
        return varNames;
    }

    public  void setAlias( String alias, int col )
    {
        assert ( (col >= 0) && (col < columns.size()) );
        aliasToColumn.put( alias, columns.get(col));
    }

    public int getAliasPosition( String alias ) {
        if ( aliasToColumn.containsKey( alias )) {
            return aliasToColumn.get( alias ).position;
        }
        else {
            return -1;
        }
    }

    public void reduceColumns( ArrayList<Integer> keepColumns ) {
        int currColumn = 0;
        Iterator<ColumnData> colIt = columns.iterator();
        while (colIt.hasNext()) {
            ColumnData colData = colIt.next();
            assert(currColumn == colData.position);
            if (keepColumns.contains(colData.position))
            {
                int reducedPosition = keepColumns.indexOf(colData.position);
                // Keep this column
                //logger.debug("Keeping column "+colData.position+" as "+reducedPosition);
                colData.position = reducedPosition;
            }
            else {
                // Remove it
                //logger.debug("Removing column "+colData.position);
                String nameStr = null;
                switch (colData.cType) {
                    case VAR_COLUMN:
                        nameStr = varNameToKey(colData.name);
                        break;
                    case ATTR_COLUMN:
                        nameStr = attrNameToKey(colData.name, colData.secondName);
                        break;
                    case LABEL_COLUMN:
                        nameStr = labelNameToKey(colData.name);
                        break;
                    case OTHER_COLUMN:
                        nameStr = otherNameToKey(colData.name);
                        break;
                    case EXPRESSION_COLUMN:
                        nameStr = exprNameToKey(colData.name);
                        break;
                    case AGGREGATE_COLUMN:
                        nameStr = aggrNameToKey(colData.name, colData.secondName);
                        break;
                }
                nameToColumn.remove(nameStr, colData);
                Iterator<Map.Entry<String,ColumnData>> aliastIt= aliasToColumn.entrySet().iterator();
                while (aliastIt.hasNext()) {
                    Map.Entry<String,ColumnData> entry = aliastIt.next();
                    if (entry.getValue() == colData) {
                        aliastIt.remove();
                    }
                }
                colIt.remove();
            }
            currColumn++;
        }
    }


    public int getAggregateColumnIndex( String aggExpr ) {
        String str = "AGGR//"+aggExpr;
        if ( nameToColumn.containsKey( str )) {
            return nameToColumn.get( str ).position;
        }
        else {
            return -1;
        }
    }

}

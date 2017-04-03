package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.PgqlException;
import oracle.pgql.lang.Pgql;
import oracle.pgql.lang.PgqlResult;


import oracle.pgql.lang.ir.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 */
public class SparkseePGQL {

    SparkseePGQL()
    {
        try {
            pgql = new Pgql();
        }
        catch (PgqlException ex)
        {
        }
    }

    final Logger logger = LoggerFactory.getLogger(SparkseePGQL.class);

    private Pgql pgql;
    private PgqlResult pgqlResult;
    private GraphQuery pgqlGraphQuery;

    private boolean setPGQLQuery(String query) throws PgqlException
    {
        pgqlResult= pgql.parse(query);
        if (pgqlResult.isQueryValid())
        {
            pgqlGraphQuery = pgqlResult.getGraphQuery();
            //System.out.println(pgqlGraphQuery);
            return true;
        }
        else
        {
            //System.out.println(pgqlResult.getErrorMessages());
            return false;
        }
    }

    private void printPGQLQuery()
    {
        if (pgqlResult.isQueryValid())
        {
            System.out.println(pgqlGraphQuery);
        }
        else
        {
            System.out.println(pgqlResult.getErrorMessages());
        }
    }


    private HashMap<String, VariableInfo> allVariables;
    private ArrayList<ConnectionGroup> connGroups;
    private ArrayList<ConstraintInfo> constraintInfoList;

    private SQAQueryBuilder finalQuery;

    private ArrayList<Integer> groupColumns;
    private ArrayList<PostGroupExprColumnInfo> projectionColumns;
    private ArrayList<AggregateInfo> aggregateInfos;
    private ArrayList<PostGroupExprColumnInfo> orderColumns;
    private ArrayList<Integer> selectedColumns;


    /**
     * \brief Creates the allVariables and connGroups information for the current pgqlGraphQuery.
     */
    protected void extractConnectionInfo()
    {
        // CONNECTIONS
        int counter = 0;
        allVariables = new HashMap<String, VariableInfo>();
        connGroups = new ArrayList<ConnectionGroup>();
        for(VertexPairConnection conn : pgqlGraphQuery.getGraphPattern().getConnections()){
            String srcName = conn.getSrc().getName();
            allVariables.put(srcName, new VariableInfo(srcName, conn.getSrc().isAnonymous(), true));
            String dstName = conn.getDst().getName();
            allVariables.put(dstName, new VariableInfo(dstName, conn.getDst().isAnonymous(), true));
            String edgeName = conn.getName();
            allVariables.put(edgeName, new VariableInfo(edgeName, conn.isAnonymous(), false));

            logger.debug("Connection "+counter+": Source \""+srcName+"\", Destination \""+dstName+"\", Edge \""+edgeName+"\".\n");
            ConnectionGroup connGroup = new ConnectionGroup();
            connGroup.add(conn);
            connGroups.add(connGroup);
            ++counter;
        }
    }

    protected void groupConnections()
    {
        // DIRECT CONNECTION GROUPS
        int counter = 0;
        // Merge the connection groups that can be directly linked by variables used in the connections.
        // The connections in each group will be linked by simple explode/neighbors operations.
        ConnectionGroup.groupConnections(connGroups);
        // LOG the groups information
        for(ConnectionGroup connGroup: connGroups)
        {
            logger.debug("Connection GROUP "+counter+": "+connGroup.toString()+"\n");
            ++counter;
        }
    }

    protected void addGroupsWithoutConnection() {
        // Other groups
        // This is required when there is a group without any connection like: (n)
        // We consider independent groups the variables that are not part of any connection.
        // TODO: Check this again when PATH support is added.
        for ( QueryVertex qv: pgqlGraphQuery.getGraphPattern().getVertices() ) {
            if (!qv.isAnonymous() && !allVariables.containsKey(qv.getName())) {
                logger.debug("Variable that doesn't appear in any connection: "+qv.getName());
                ConnectionGroup connGroup = new ConnectionGroup();
                connGroup.setNodeOnlyGroup( qv.getName() );
                connGroups.add(connGroup);
                allVariables.put(qv.getName(), new VariableInfo(qv.getName(), false, true));
            }
        }
    }

    protected  void extractConstraintsInfo()
    {
        // CONSTRAINTS
        int counter = 0;
        constraintInfoList = new ArrayList<ConstraintInfo>();
        for(QueryExpression expr : pgqlGraphQuery.getGraphPattern().getConstraints()){
            ConstraintInfo info = new ConstraintInfo(expr);
            constraintInfoList.add(info);
            logger.debug("Constraint "+counter+": "+ info.toString() );
            ++counter;
        }
    }

    protected  void linkConstraintsToGroups()
    {
        // link the constraints to the groups that use the same variables
        for (ConstraintInfo constraintInfo : constraintInfoList) {
            constraintInfo.linkGroups(connGroups);
        }
    }

    protected void translateGroups() {
        // Translate each individual group to the sparksee algebra
        for (ConnectionGroup connectionGroup : connGroups) {
            // Proves de traduccio a l'algebra
            SQAQueryBuilder groupQuery = translateConnectionGroup(connectionGroup);
            connectionGroup.setSQAQuery(groupQuery);
            logger.debug("TanslatedConnectionGroup: " + groupQuery.getQuery());
        }
    }

    protected void joinGroupsByConstraint() {
        // JOIN the group pairs that can be connected through a constraint
        boolean groupsToJoin = true;
        while (groupsToJoin) {
            groupsToJoin = false;
            Iterator<ConstraintInfo> cinfoIter = constraintInfoList.iterator();
            while (cinfoIter.hasNext()) {
                ConstraintInfo cinfo = cinfoIter.next();
                if (cinfo.getNumLinkedGroups() == 2) {
                    // The two groups can be joined into one
                    ConnectionGroup g1 = cinfo.getFirstLinkedGroup();
                    ConnectionGroup g2 = cinfo.getGroups().get(1);
                    // Apply the JOIN constraint
                    cinfo.applyJOINConstraint(g1.getSQAQuery(), g2.getSQAQuery());
                    // Add the g2 information to g1
                    g1.merge(g2);
                    // Remove the current constraint
                    cinfoIter.remove();
                    // The g2 group must be removed from the groups list and
                    // replaced in any constraintInfo by g1 (or just removed if g1 was already used)
                    connGroups.remove(g2);
                    for (ConstraintInfo cginfo: constraintInfoList) {
                        cginfo.mergedGroups(g1, g2);
                    }
                    // More constraints may be joinable with this group reduction
                    groupsToJoin = true; // Loop again
                }
                else if (cinfo.getNumLinkedGroups() == 1) {
                    // This could only happen if this constraint was using the same groups of another constraint
                    // that had already been joined into one group.
                    // So this constraint can simply be applied now.
                    cinfo.applyConstraint( cinfo.getFirstLinkedGroup().getSQAQuery() );
                    // Remove the current constraint
                    cinfoIter.remove();
                }
            }
        }
    }


    protected void mergeGroupsWithProduct() {
        // The final remaining unrelated groups will be linked using PRODUCT operations.
        // If any constraint had more than 2 groups it can be linked with a product as well,
        // but later the constraint must be applied.
        if (connGroups.size() > 1) {
            Iterator<ConnectionGroup> groupIter = connGroups.iterator();
            assert (groupIter.hasNext());
            ConnectionGroup first = groupIter.next();
            SQAQueryBuilder sqaQuery = first.getSQAQuery();
            while (groupIter.hasNext()) {
                ConnectionGroup group = groupIter.next();
                SQAQueryBuilder sqaQuery2 = group.getSQAQuery();
                // Apply the constraint
                sqaQuery.preppendQuery("PRODUCT( ");
                sqaQuery.appendQuery(", " +sqaQuery2.getQuery()+" )" );
                // Add to the final query all the columns of the second query
                sqaQuery.addColumnsData(sqaQuery2);
                // Add the g2 information to g1
                first.merge(group);
                // The second group must be removed from the groups list and
                // replaced in any constraintInfo by the first one
                groupIter.remove();
                for (ConstraintInfo cginfo: constraintInfoList) {
                    cginfo.mergedGroups(first, group);
                }
            }
        }
        assert(connGroups.size() == 1);
        logger.debug("FINAL GROUP ALGEBRA: " + connGroups.get(0).getSQAQuery().getQuery());
    }

    protected void applyMultipleGroupConstraints() {
        // Any constraint that was using variables for more than 2 groups could not be joined
        // but are now linked by a PRODUCT. So the remaining constraints can simply be applied.
        Iterator<ConstraintInfo> cinfoIter = constraintInfoList.iterator();
        while (cinfoIter.hasNext()) {
            ConstraintInfo cinfo = cinfoIter.next();
            assert((cinfo.getNumLinkedGroups() == 1) && (connGroups.get(0) == cinfo.getFirstLinkedGroup()));
            cinfo.applyConstraint( cinfo.getFirstLinkedGroup().getSQAQuery() );
            // Remove the current constraint
            cinfoIter.remove();
        }
        logger.debug("FINAL GROUP CONSTRAINED ALGEBRA: " + connGroups.get(0).getSQAQuery().getQuery() );
    }


    protected  void prepareGroupBy() {
        // The GROUPBY can't be applied until the SELECT and ORDERBY are checked
        // because both the ORDERBY and the SELECT may contain aggregations that
        // we must know to write the SQA GROUP operation. But the GROUPBY must be
        // checked first anyway because the ORDERBY and SELECT may contain alias
        // defined in the GROUPBY.
        GroupBy groupBy = pgqlGraphQuery.getGroupBy();
        groupColumns = new ArrayList<Integer>();
        if(!groupBy.getElements().isEmpty()) {
            Iterator it = groupBy.getElements().iterator();
            while(it.hasNext()) {
                ExpAsVar expAsVar = (ExpAsVar) it.next();
                logger.debug("Must group by "+expAsVar.getName());
                logger.debug("\tWith the expression: "+expAsVar.getExp().toString());
                ExprColumnInfo eInfo = new ExprColumnInfo(expAsVar.getExp());
                logger.debug("Info groupby: "+ eInfo.toString());

                assert( ! eInfo.hasAggregates() );
                int colNumber = eInfo.addExpressionColumn( finalQuery, expAsVar.getName() );
                assert( colNumber >= 0 && colNumber < finalQuery.getNumColumns() );
                groupColumns.add( colNumber );
                // Guardar la posició no es suficient perque si la columna del group by es una variable
                // llavors atributs d'aquesta variable poden ser utilitzats a l'orderby o la projeccio.
                // El problema es que el groupby de l'algebra haura eliminat totes les columnes
                // que no siguin les d'agrupaciio o les de calcul d'agregats. Per tant, encara que
                // ja s'hagues obtingut l'atribut, el perdrem a no ser que es modifiqui el groupby.
                // Segurament seria mes eficient no haver-lo obtingut si no calia per alguna altra
                // operacio i obtenir-lo només a l'hora de fer l'orderby/projection.
                // Pero per fer aixo cal saber quines de les columnes de sortida del groupby seran
                // variables (i a quina variable corresponen).
                // Ben mirat potser si que n'hi ha prou amb la posicio perque es pot consultar
                // al ColumnData del SQAQueryBuilder si es una variable o no.
            }
        }
        logger.debug("FINAL GROUP EXPRESSIONS QUERY: " + finalQuery.getQuery() );
    }

    protected void prepareSelect() {
        // The projection must be checked before the ORDERBY because the alias defined in the
        // SELECT can be used in the ORDERBY. And it can not be applied until the GROUPBY
        // is done because the SELECT may use aggregate operations.
        Projection projection = pgqlGraphQuery.getProjection();
        projectionColumns = new ArrayList<PostGroupExprColumnInfo>();
        aggregateInfos = new ArrayList<AggregateInfo>();
        if(projection.getElements().isEmpty()) {
            // TODO: *
        } else {
            Iterator it = projection.getElements().iterator();
            while(it.hasNext()) {
                ExpAsVar expAsVar = (ExpAsVar) it.next();
                logger.debug("SELECTED column: "+expAsVar.getName());
                logger.debug("\tWith the expression: "+expAsVar.getExp().toString());
                // Get information from the projection
                PostGroupExprColumnInfo pgInfo = new PostGroupExprColumnInfo(expAsVar.getExp());
                logger.debug("Projection info: "+ pgInfo.toString());
                pgInfo.extractAggregatesInfo( finalQuery, aggregateInfos );
                // Keep the PostGroupExprColumnInfo for later, when the projection is really done
                projectionColumns.add(pgInfo);
            }
        }
    }

    protected void prepareOrderBy() {
        // It can not be applied until the GROUPBY is done because it may use aggregate operations
        // but it must be checked first to get it's requirements.
        OrderBy orderBy = pgqlGraphQuery.getOrderBy();
        orderColumns = new ArrayList<PostGroupExprColumnInfo>();
        if(!orderBy.getElements().isEmpty()) {
            Iterator it = orderBy.getElements().iterator();
            while(it.hasNext()) {
                OrderByElem orderByElem = (OrderByElem) it.next();
                logger.debug("Order by: "+orderByElem.toString());
                logger.debug("\tWith the expression: "+orderByElem.getExp().toString());
                // Get OrderBy information
                PostGroupExprColumnInfo pgInfo = new PostGroupExprColumnInfo(orderByElem.getExp());
                logger.debug("OrderBy info: "+ pgInfo.toString());
                pgInfo.extractAggregatesInfo( finalQuery, aggregateInfos );
                // Keep the PostGroupExprColumnInfo for later, when the orderby is really done
                orderColumns.add(pgInfo);
            }
        }
    }

    protected void applyGroupBy() {
        if ( ! groupColumns.isEmpty() ) {
            finalQuery.preppendQuery("GROUP( ");
            finalQuery.appendQuery(", [ ");
            boolean first = true;
            for (Integer groupCol : groupColumns) {
                if (!first) {
                    finalQuery.appendQuery(", ");
                }
                finalQuery.appendQuery(groupCol.toString());
                first = false;
            }
            finalQuery.appendQuery(" ], [ ");
            if (aggregateInfos.isEmpty()) {
                finalQuery.appendQuery("null");
            }
            else {
                first = true;
                for (AggregateInfo aggInfo : aggregateInfos) {
                    if (!first) {
                        finalQuery.appendQuery(", ");
                    }
                    finalQuery.appendQuery(aggInfo.getGroupSQAExpression());
                    first = false;
                }
            }
            finalQuery.appendQuery(" ])");
            // Remove all the columns data in the SQAQueryBuilder except the group columns
            finalQuery.reduceColumns(groupColumns);
            // Add the calculated aggregate columns
            for (AggregateInfo aggInfo : aggregateInfos) {
                finalQuery.addAggregateColumn(aggInfo.getType(), aggInfo.getSubexpr());
            }
        }
    }

    protected void addProjectionExpressionColumns() {
        // We must create all the projection columns and consider every
        // "column expression" as an alias.
        // Later we will order them and finally we will remove the not wanted columns.
        selectedColumns = new ArrayList<Integer>();
        for (PostGroupExprColumnInfo postExpr : projectionColumns) {
            // We need to parse the expression but without going in the aggregate expressions
            postExpr.parseQueryExpression();
            // Add the new column or find the existing one
            int numCol = postExpr.addExpressionColumn(finalQuery, null);
            finalQuery.setAlias(postExpr.expression.toString(), numCol);
            logger.debug("Adding projection alias: \""+postExpr.expression.toString()+"\" col "+numCol);
            selectedColumns.add(numCol);
        }
    }

    protected void applyOrderBy() {
        ArrayList<Integer> sortColumns = new ArrayList<Integer>();;
        for (PostGroupExprColumnInfo postExpr : orderColumns) {
            // We need to parse the expression but without going in the aggregate expressions
            postExpr.parseQueryExpression();
            // Add the new column or find the existing one
            int numCol = postExpr.addExpressionColumn(finalQuery, null);
            logger.debug("Order by expression: \""+postExpr.expression.toString()+"\" col "+numCol);
            assert(numCol >= 0);
            sortColumns.add(numCol);
        }
        if (!sortColumns.isEmpty()) {
            finalQuery.preppendQuery("SORT(");
            finalQuery.appendQuery(", [ ");
            boolean firstColumn = true;
            for (Integer numCol : sortColumns) {
                if (!firstColumn) {
                    finalQuery.appendQuery(", ");
                }
                finalQuery.appendQuery( numCol.toString() );
                firstColumn = false;
            }
            finalQuery.appendQuery( " ])");
        }
    }

    protected void applySelect() {
        if (!selectedColumns.isEmpty()) {
            finalQuery.preppendQuery("PROJECT(");
            finalQuery.appendQuery(", [ ");
            boolean firstColumn = true;
            for (Integer numCol : selectedColumns) {
                if (!firstColumn) {
                    finalQuery.appendQuery(", ");
                }
                finalQuery.appendQuery( numCol.toString() );
                firstColumn = false;
            }
            finalQuery.appendQuery(" ])");
            finalQuery.reduceColumns(selectedColumns);
            // TODO: Set the right names to the result columns
        }
    }

    protected void limitAndOffset() {
        long limit = pgqlGraphQuery.getLimit();
        long offset = pgqlGraphQuery.getOffset();
        if ((limit > -1L) && (offset > -1L)) {
            finalQuery.preppendQuery("SLICE( ");
            finalQuery.appendQuery(", "+offset+", "+limit+" )");
        }
        else if (limit > -1L) {
            finalQuery.preppendQuery("SLICE( ");
            finalQuery.appendQuery(", NULL, "+limit+" )");
        }
        else if (offset > -1L) {
            finalQuery.preppendQuery("SLICE( ");
            finalQuery.appendQuery(", "+offset+", NULL )");
        }
    }

    protected ConstraintInfo findBestInitialConstraint( ConnectionGroup connGroup)
    {
        ConstraintInfo best = null;
        for (ConstraintInfo constraintInfo : constraintInfoList) {
            //TODO: A size estimation would be useful
            if ( constraintInfo.isABasicOperation() ) {
                // Only one variable to check
                String name = constraintInfo.getFirstVariableName();
                if (connGroup.containsVariable(name))
                {
                    if (best == null) {
                        best = constraintInfo;
                    }
                    else if (allVariables.get(name).isEdge() && allVariables.get(best.getFirstVariableName()).isNode()) {
                        best = constraintInfo;
                    }
                }
            }
        }
        return best;
    }


    /**
     * Updates the query to add the given connection using the edge variable.
     * @param query [in/out] The query to update
     * @param vpConn [in] The new connection
     * @return Rerturns an array with the name of the added variable columns
     */
    protected ArrayList<String>  translateConnectionByEdge(SQAQueryBuilder query, VertexPairConnection vpConn )
    {
        boolean filterSrc = false;
        boolean filterDst = false;
        int initialNumColumns = query.getNumColumns();
        ArrayList<String> addedVariableColumns = new ArrayList<String>();
        query.preppendQuery("GRAPH::ADJACENT(");
        query.appendQuery(", "+query.getVariableColumnIndex(vpConn.getName())+")");
        if (query.containsVariable(vpConn.getSrc().getName())) {
            // The first new column is a variable that we already have, so it should be added
            // as other and removed later
            query.addOtherColumn("VARIABLE_DUPLICADA:"+vpConn.getSrc().getName() );
            filterSrc = true;
        }
        else {
            query.addVariableColumn(vpConn.getSrc().getName());
            addedVariableColumns.add(vpConn.getSrc().getName());
        }
        if (query.containsVariable(vpConn.getDst().getName())) {
            // The second new column is a variable that we already have, so it should be added
            // as other and removed later
            query.addOtherColumn("VARIABLE_DUPLICADA:"+vpConn.getDst().getName() );
            filterDst = true;
        }
        else {
            query.addVariableColumn(vpConn.getDst().getName());
            addedVariableColumns.add(vpConn.getDst().getName());
        }

        // Select only the records where the new columns match the values we already had
        if ( filterSrc && filterDst )
        {
            // Select all in a single SELECT operation
            query.preppendQuery("SELECT(");
            query.appendQuery(", (%"+
                    query.getVariableColumnIndex(vpConn.getSrc().getName())+"=%"+initialNumColumns+
                    ") AND (%"+query.getVariableColumnIndex(vpConn.getDst().getName())+"=%"+(initialNumColumns+1)+
                    ") )");
        }
        else if (filterSrc) {
            query.preppendQuery("SELECT(");
            query.appendQuery(", %"+
                    query.getVariableColumnIndex(vpConn.getSrc().getName())+"=%"+initialNumColumns+
                    " )");
        }
        else if (filterDst) {
            query.preppendQuery("SELECT(");
            query.appendQuery(", %"+
                    query.getVariableColumnIndex(vpConn.getDst().getName())+"=%"+(initialNumColumns+1)+
                    " )");
        }

        return addedVariableColumns;
    }


    /**
     *
     * Updates the query to add the given connection using one of the nodes.
     * @param query [in/out] The query to update
     * @param vpConn [in] The new connection
     * @param bySrc [in] True if the source node is used or false to use the destination
     * @return  Rerturns an array with the name of the added variable columns
     */
    protected ArrayList<String>  translateConnectionByNode(SQAQueryBuilder query, VertexPairConnection vpConn, boolean bySrc)
    {
        boolean filterEdge = false;
        boolean filterPeer = false;
        int initialNumColumns = query.getNumColumns();
        ArrayList<String> addedVariableColumns = new ArrayList<String>();

        String startName = (bySrc? vpConn.getSrc().getName() : vpConn.getDst().getName());
        String peerName = (bySrc? vpConn.getDst().getName() : vpConn.getSrc().getName());
        String direction = (bySrc? "OUTGOING" : "INGOING");

        query.preppendQuery("GRAPH::EXPLODE(");
        query.appendQuery(", "+query.getVariableColumnIndex(startName)+
                        ", [ ALL "+direction+" ]"+
                        ", {'neighbor'=true} )");
        if (query.containsVariable(vpConn.getName())) {
            // The first new column (the edge) is a variable that we already have, so it should be added
            // as other and removed later
            query.addOtherColumn("VARIABLE_DUPLICADA:"+vpConn.getName() );
            filterEdge = true;
        }
        else {
            query.addVariableColumn(vpConn.getName());
            addedVariableColumns.add(vpConn.getName());
        }
        if (query.containsVariable( peerName )) {
            // The second new column (the peer node) is a variable that we already have, so it should be added
            // as other and removed later
            query.addOtherColumn("VARIABLE_DUPLICADA:"+ peerName );
            filterPeer = true;
        }
        else {
            query.addVariableColumn(peerName);
            addedVariableColumns.add(peerName);
        }

        // Select only the records where the new columns match the values we already had
        if ( filterEdge && filterPeer )
        {
            // Select all in a single SELECT operation
            query.preppendQuery("SELECT(");
            query.appendQuery(", (%"+
                    query.getVariableColumnIndex(vpConn.getName())+"=%"+initialNumColumns+
                    ") AND (%"+query.getVariableColumnIndex(peerName)+"=%"+(initialNumColumns+1)+
                    ") )");
        }
        else if (filterEdge) {
            query.preppendQuery("SELECT(");
            query.appendQuery(", %"+
                    query.getVariableColumnIndex(vpConn.getName())+"=%"+initialNumColumns+
                    " )");
        }
        else if (filterPeer) {
            query.preppendQuery("SELECT(");
            query.appendQuery(", %"+
                    query.getVariableColumnIndex(peerName)+"=%"+(initialNumColumns+1)+
                    " )");
        }

        return addedVariableColumns;
    }


    /**
     * Updates the already calculated variables for all the constraints and apply
     * the simple constraints (only linked to 1 group) of the given group that
     * already have all the variables calculated.
     * @param query [in/out] The query to update.
     * @param addedVars [in] The new variables already available.
     * @param group [in] the current group.
     */
    protected void updateAndApplyConstraintVarsInGroup(SQAQueryBuilder query, ArrayList<String>  addedVars, ConnectionGroup group) {
        if (addedVars != null) {
            // All the constraints that use only variables used in the query can already be applied
            Iterator<ConstraintInfo> cinfoIter = constraintInfoList.iterator();
            while (cinfoIter.hasNext()) {
                ConstraintInfo cinfo = cinfoIter.next();
                cinfo.updatePendingVariables(addedVars);
                if (cinfo.getNumPendingVariables() == 0) {
                    // All the variables used in the constraint are already available
                    if ( (cinfo.getNumLinkedGroups() == 1) && (cinfo.getFirstLinkedGroup() == group)) {
                        cinfo.applyConstraint(query);
                        // Remove the constraint
                        cinfoIter.remove();
                    }
                }
            }
        }
    }

    /**
     * Translate a connection group to a Sparksee Algebra Query.
     * The Query only translates the connection and constraints contained in the group, without
     * joining with any other group.
     * @param group [in] The connection group to translate
     * @return Returns a Sparksee Query Algebra for the group connections and exclusive constraints
     */
    protected SQAQueryBuilder translateConnectionGroup( ConnectionGroup group )
    {
        SQAQueryBuilder query = null;
        ConstraintInfo bestInitialConstraint = findBestInitialConstraint(group);
        if (bestInitialConstraint == null) {
            logger.debug("Starting with a simple scan.");
            String varName = null;
            for (String currVar : group.getVariables()) {
                if (allVariables.get(currVar).isNode()) {
                    varName = currVar;
                    break;
                }
            }
            assert (varName != null);
            query = new SQAQueryBuilder();
            query.appendQuery("GRAPH::SCAN(NODES)");
            query.addVariableColumn(varName);
        } else {
            query = bestInitialConstraint.getAsBasicSQAOperation();
            logger.debug("Best Basic OP: " + query.getQuery() + "\n");
            // Remove the constraint from the list
            constraintInfoList.remove(bestInitialConstraint);
        }

        // The first variable is available, so other simple constraints may be applied before checking any connection
        updateAndApplyConstraintVarsInGroup( query, query.getVariables(), group);

        // Iterate the connections / constraints to complete the independent group complete query
        ArrayList<VertexPairConnection> connections = new ArrayList<VertexPairConnection>( group.getConnections()) ;
        while (!connections.isEmpty())
        {
            ArrayList<String>  addedVars = null;
            int numConnections = connections.size(); // Just for the assert

            for (VertexPairConnection vpConn: connections) {
                if ( query.containsVariable(vpConn.getName())) {
                    // The connection can be processed through the edge
                    // If we have the edge, we can easily get the other nodes or check them if we already
                    // had variables that must match the edge peers.
                    addedVars = translateConnectionByEdge(query, vpConn);
                    connections.remove(vpConn);
                    break;
                }
                else if (query.containsVariable(vpConn.getSrc().getName())) {
                    // The connection can be processed through the source node
                    addedVars = translateConnectionByNode(query, vpConn, true);
                    connections.remove(vpConn);
                    break;
                }
                else if (query.containsVariable(vpConn.getDst().getName())) {
                    // The connection can be processed through the destination node
                    addedVars = translateConnectionByNode(query, vpConn, false);
                    connections.remove(vpConn);
                    break;
                }

            }
            assert(connections.size() < numConnections); // To avoid an infinite loop while testing

            if (addedVars != null) {
                // All the constraints that use only variables used in the query can already be applied
                updateAndApplyConstraintVarsInGroup( query, addedVars, group);
            }
        }

        return query;
    }


    /**
     * Translate the given PGQL query to Sparksee Query Algebra.
     * @param query [in] The PGQL query string
     * @return Returns a String with the SQA query translation.
     */
    public String ProcessPGQLQuery(String query) {
        logger.debug("--------------------------------------------------");
        logger.debug("Will process query \""+query+"\"");
        try {
            if (!setPGQLQuery(query)) {
                printPGQLQuery();
                return null;
            }
        } catch (PgqlException ex) {
            return null;
        }

        // --------------------------------------------------
        // Build connection groups and constraints
        // --------------------------------------------------
        extractConnectionInfo();
        groupConnections();
        addGroupsWithoutConnection();
        extractConstraintsInfo();
        linkConstraintsToGroups();

        // --------------------------------------------------
        // LOG the variables information
        // --------------------------------------------------
        int counter = 0;
        for (Map.Entry<String, VariableInfo> varEntry : allVariables.entrySet()) {
            logger.debug("Variable " + counter + ": " + varEntry.getValue().toString() + "\n");
            ++counter;
        }

        // --------------------------------------------------
        // Merge groups into a single base algebra query
        // --------------------------------------------------
        // Translate each individual group to the sparksee algebra
        translateGroups();
        // JOIN the group pairs that can be connected through a constraint
        joinGroupsByConstraint();
        // Link the remaining unrelated groups using PRODUCT operations
        mergeGroupsWithProduct();
        // Apply the remaining constraints
        applyMultipleGroupConstraints();
        // The only group remaining query algebra builder will became our final query
        finalQuery = connGroups.get(0).getSQAQuery();

        // --------------------------------------------------
        // Prepare the GROUP BY
        // --------------------------------------------------
        prepareGroupBy();
        prepareSelect();
        prepareOrderBy();

        // --------------------------------------------------
        // Apply the GROUP BY
        // --------------------------------------------------
        applyGroupBy();

        // --------------------------------------------------
        // Add the final PROJECTION expression columns before the OrderBy
        // --------------------------------------------------
        addProjectionExpressionColumns();

        // --------------------------------------------------
        // Apply the ORDER BY
        // --------------------------------------------------
        applyOrderBy();

        // --------------------------------------------------
        // Apply the FINAL PROJECTION
        // --------------------------------------------------
        applySelect();

        // --------------------------------------------------
        // LIMIT and OFFSET
        // --------------------------------------------------
        limitAndOffset();



        logger.debug("--------------------------------------------------");
        logger.debug("SPARKSEE ALGEBRA QUERY:");
        logger.debug(finalQuery.getQuery());
        logger.debug("--------------------------------------------------");
        return finalQuery.getQuery();
    }

}


package com.sparsity.SparkseePGQL;

import oracle.pgql.lang.ir.VertexPairConnection;

import java.util.ArrayList;
import java.util.HashSet;

/**
 */
public class ConnectionGroup {
    private ArrayList<VertexPairConnection> connections;
    private HashSet<String> variables;
    private SQAQueryBuilder sqaQuery;

    public ConnectionGroup() {
        connections = new ArrayList<VertexPairConnection>();
        variables = new HashSet<String>();
        sqaQuery = null;
    }

    public void add(VertexPairConnection conn) {
        connections.add(conn);
        if (!variables.contains( conn.getName() )) {
            variables.add( conn.getName() );
        }
        if (!variables.contains( conn.getSrc().getName())) {
            variables.add( conn.getSrc().getName());
        }
        if (!variables.contains( conn.getDst().getName())) {
            variables.add( conn.getDst().getName());
        }
    }

    public boolean canBeConnected( ConnectionGroup connGroup ) {
        for (String name: connGroup.variables)
        {
            if (variables.contains(name)) {
                return true;
            }
        }
        return false;
    }

    public void merge( ConnectionGroup connGroup )
    {
        for (VertexPairConnection conn: connGroup.connections)
        {
            connections.add(conn);
        }
        for (String name: connGroup.variables)
        {
            if (!variables.contains(name)) {
                variables.add(name);
            }
        }
    }


    public static void groupConnections( ArrayList<ConnectionGroup> connGroups )
    {
        int prevNumGroups = 0;
        int numGroups = connGroups.size();
        while ( (numGroups != prevNumGroups) && (numGroups > 1) )
        {
            for (int ii=0; ii< connGroups.size(); ii++)
            {
                ConnectionGroup cg1 = connGroups.get(ii);
                for (int jj=ii+1; jj< connGroups.size(); )
                {
                    ConnectionGroup cg2 = connGroups.get(jj);
                    if (cg1.canBeConnected(cg2)) {
                        cg1.merge(cg2);
                        connGroups.remove(jj);
                    }
                    else {
                        jj++;
                    }
                }
            }
            prevNumGroups = numGroups;
            numGroups = connGroups.size();
        }
    }

    @Override
    public String toString() {
        return "ConnectionGroup{" +
                "connections=" + connections +
                ", variables=" + variables +
                '}';
    }

    public boolean containsVariable( String name ) {
        return variables.contains(name);
    }

    public ArrayList<VertexPairConnection> getConnections() {
        return connections;
    }

    public SQAQueryBuilder getSQAQuery() {
        return sqaQuery;
    }

    public void setSQAQuery(SQAQueryBuilder sqaQuery) {
        this.sqaQuery = sqaQuery;
    }

    public HashSet<String> getVariables() {
        return variables;
    }

    public void setNodeOnlyGroup( String varName ) {
        assert (connections.isEmpty() && variables.isEmpty());
        variables.add( varName );
        //sqaQuery = new SQAQueryBuilder();
        //sqaQuery.appendQuery("GRAPH::SCAN(NODES)");
    }
}

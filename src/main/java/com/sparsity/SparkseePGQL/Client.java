package com.sparsity.SparkseePGQL;

import com.sparsity.sparksee.gdb.*;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 */
public class Client {

    private SparkseePGQL spgql;
    private String dbFilePath;
    private Boolean showAlgebra;
    private Sparksee sparksee;
    private Database db;
    private Session sess;
    private Graph graph;
    private int resultRowsLimit;


    public Client() {
        spgql = new SparkseePGQL();
        dbFilePath = null;
        showAlgebra = false;
        sparksee = null;
        db = null;
        sess = null;
        graph = null;
        resultRowsLimit = 10;
    }

    @Override
    protected void finalize() throws Throwable {
        if (sess != null) {
            sess.close();
            sess = null;
        }
        if (db != null) {
            db.close();
            db = null;
        }
        if (sparksee != null) {
            sparksee.close();
            sparksee = null;
        }
        super.finalize();
    }

    public void setShowAlgebra(Boolean showAlgebra) {
        this.showAlgebra = showAlgebra;
    }


    /**
     * Opens the given database as read-only
     * @param databaseFilePath
     * @return Returns true if the DB is successfully open or false otherwise.
     */
    public boolean openDatabase( String databaseFilePath ) {
        dbFilePath = databaseFilePath;
        SparkseeConfig cfg = new SparkseeConfig();
        sparksee = new Sparksee(cfg);
        try {
            db = sparksee.open(dbFilePath, true);
        } catch (java.io.FileNotFoundException ex) {
            System.err.println("Error opening Sparksee database \""+dbFilePath+"\": "+ex.getMessage());
            return false;
        }
        sess = db.newSession();
        graph = sess.getGraph();
        return true;
    }


    public void runSparkseeQuery( String queryStr ) {
        Value v = new Value();
        Query query = sess.newQuery();
        //System.out.println("Running query: \""+queryStr+"\"");
        ResultSet rs = query.execute(queryStr);
        String result = rs.getJSON(resultRowsLimit);
        System.out.println("Sparksee query execution result:\n"+result);
        rs.close();
        query.close();
    }

    public void processScript( String queriesFile ) {
        int counter = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(queriesFile))) {
            String line;
            Boolean queryReady = false;
            StringBuilder pgqlQueryString = new StringBuilder();
            while ((line = br.readLine()) != null) {
                // process the line.
//                line.replace("\n", " ");
//                line.replace("\r", "");
                if (line.compareTo("?")==0)
                {
                    queryReady = true;
                }
                else if (line.startsWith("#"))
                {
                    continue;
                }
                else if (line.endsWith("?"))
                {
                    pgqlQueryString.append(line.substring(0,line.length()-2));
                    queryReady = true;
                }
                else {
                    if ( pgqlQueryString.length() > 0 ) {
                        pgqlQueryString.append(" ");
                    }
                    pgqlQueryString.append(line);
                }
                if (queryReady)
                {
                    // Show the original PGQL query:
                    System.out.println("----------------------------------------------------------------------");
                    System.out.println("Query "+counter);
                    System.out.println("----------------------------------------------------------------------");
                    System.out.println("Source PGQL Query:\n"+pgqlQueryString.toString());
                    String sqaQuery = spgql.ProcessPGQLQuery(pgqlQueryString.toString());
                    if (showAlgebra) {
                        // Shown the algebra translation
                        System.out.println( "Translated Sparksee Query Algebra:\n"+sqaQuery);
                    }
                    if (sess != null) {
                        // Run the query
                        runSparkseeQuery(sqaQuery);
                    }
                    pgqlQueryString.setLength(0);
                    queryReady = false;
                    counter++;
                }
            }
        }
        catch (IOException ex)
        {
            System.err.println("Error reading input file \""+queriesFile+"\".");
        }

    }

    private static void loadData( com.sparsity.sparksee.gdb.Graph g) {
        //
        // SCHEMA
        //

        // Add a node type for the movies, with a unique identifier and two indexed attributes
        int movieType = g.newNodeType("MOVIE");
        int movieIdType = g.newAttribute( Type.GlobalType /*movieType*/, "ID", DataType.Long, AttributeKind.Unique);
        int movieTitleType = g.newAttribute(Type.GlobalType /*movieType*/, "TITLE", DataType.String, AttributeKind.Indexed);
        int movieYearType = g.newAttribute(Type.GlobalType /*movieType*/, "YEAR", DataType.Integer, AttributeKind.Indexed);

        // Add a node type for the people, with a unique identifier and an indexed attribute
        int peopleType = g.newNodeType("PEOPLE");
        //int peopleIdType = g.newAttribute(Type.GlobalType /*peopleType*/, "ID", DataType.Long, AttributeKind.Unique);
        int peopleIdType = movieIdType;
        int peopleNameType = g.newAttribute(Type.GlobalType /*peopleType*/, "NAME", DataType.String, AttributeKind.Indexed);

        // Add an undirected edge type with an attribute for the cast of a movie
        int castType = g.newEdgeType("CAST", false, false);
        int castCharacterType = g.newAttribute(Type.GlobalType /*castType*/, "CHARACTER", DataType.String, AttributeKind.Basic);

        // Add a directed edge type restricted to go from people to movie for the director of a movie
        int directsType = g.newRestrictedEdgeType("DIRECTS", peopleType, movieType, false);


        //
        // DATA
        //

        // Add some MOVIE nodes
        Value value = new Value();

        long mLostInTranslation = g.newNode(movieType);
        g.setAttribute(mLostInTranslation, movieIdType, value.setLong(1));
        g.setAttribute(mLostInTranslation, movieTitleType, value.setString("Lost in Translation"));
        g.setAttribute(mLostInTranslation, movieYearType, value.setInteger(2003));

        long mVickyCB = g.newNode(movieType);
        g.setAttribute(mVickyCB, movieIdType, value.setLong(2));
        g.setAttribute(mVickyCB, movieTitleType, value.setString("Vicky Cristina Barcelona"));
        g.setAttribute(mVickyCB, movieYearType, value.setInteger(2008));

        long mManhattan = g.newNode(movieType);
        g.setAttribute(mManhattan, movieIdType, value.setLong(3));
        g.setAttribute(mManhattan, movieTitleType, value.setString("Manhattan"));
        g.setAttribute(mManhattan, movieYearType, value.setInteger(1979));


        // Add some PEOPLE nodes
        long pScarlett = g.newNode(peopleType);
        g.setAttribute(pScarlett, peopleIdType, value.setLong(1*10000));
        g.setAttribute(pScarlett, peopleNameType, value.setString("Scarlett Johansson"));

        long pBill = g.newNode(peopleType);
        g.setAttribute(pBill, peopleIdType, value.setLong(2*10000));
        g.setAttribute(pBill, peopleNameType, value.setString("Bill Murray"));

        long pSofia = g.newNode(peopleType);
        g.setAttribute(pSofia, peopleIdType, value.setLong(3*10000));
        g.setAttribute(pSofia, peopleNameType, value.setString("Sofia Coppola"));

        long pWoody = g.newNode(peopleType);
        g.setAttribute(pWoody, peopleIdType, value.setLong(4*10000));
        g.setAttribute(pWoody, peopleNameType, value.setString("Woody Allen"));

        long pPenelope = g.newNode(peopleType);
        g.setAttribute(pPenelope, peopleIdType, value.setLong(5*10000));
        g.setAttribute(pPenelope, peopleNameType, value.setString("Penélope Cruz"));

        long pDiane = g.newNode(peopleType);
        g.setAttribute(pDiane, peopleIdType, value.setLong(6*10000));
        g.setAttribute(pDiane, peopleNameType, value.setString("Diane Keaton"));



        // Add some CAST edges
        long anEdge;
        anEdge = g.newEdge(castType, mLostInTranslation, pScarlett);
        g.setAttribute(anEdge, castCharacterType, value.setString("Charlotte"));

        anEdge = g.newEdge(castType, mLostInTranslation, pBill);
        g.setAttribute(anEdge, castCharacterType, value.setString("Bob Harris"));

        anEdge = g.newEdge(castType, mVickyCB, pScarlett);
        g.setAttribute(anEdge, castCharacterType, value.setString("Cristina"));

        anEdge = g.newEdge(castType, mVickyCB, pPenelope);
        g.setAttribute(anEdge, castCharacterType, value.setString("Maria Elena"));

        anEdge = g.newEdge(castType, mManhattan, pDiane);
        g.setAttribute(anEdge, castCharacterType, value.setString("Mary"));

        anEdge = g.newEdge(castType, mManhattan, pWoody);
        g.setAttribute(anEdge, castCharacterType, value.setString("Isaac"));



        // Add some DIRECTS edges
        anEdge = g.newEdge(directsType, pSofia, mLostInTranslation);

        anEdge = g.newEdge(directsType, pWoody, mVickyCB);

        anEdge = g.newEdge(directsType, pWoody, mManhattan);

    }

    /**
     * Creates a new sample database at the given path, loads some data and leaves the DB open.
     * @param filePath
     * @return True if the DB is created and loaded or false otherwise.
     */
    private boolean createSampleDatabase( String filePath ) {
        dbFilePath = filePath;
        // Create DB
        SparkseeConfig cfg = new SparkseeConfig();
        Sparksee sparksee = new Sparksee(cfg);
        try {
            db = sparksee.create(dbFilePath, "SparkseePGQLSampleDB");
        } catch (java.io.FileNotFoundException ex) {
            System.err.println("Error creating Sparksee database \""+dbFilePath+"\": "+ex.getMessage());
            return false;
        }
        sess = db.newSession();
        graph = sess.getGraph();
        loadData(graph);
        return true;
    }


    public static void main(String[] args) throws java.io.FileNotFoundException {

        System.out.println("SparkseePGQL Client");

        // Prepare the options
        Options options = new Options();
        Option help = new Option ( "h", "help", false,"Print help information" );
        options.addOption( help );
        Option algebra = new Option( "a", "algebra", false,"Translate to the Sparksee algebra" );
        options.addOption( algebra );
        Option createDB = new Option( "c", "createDB", false,"Create a sample Sparksee database" );
        options.addOption( createDB );
        Option dbFile   = Option.builder("db")
                .required(false)
                .longOpt("SparkseeDB")
                .desc( "The Sparksee Database file to run the queries")
                .hasArg()
                .build();
        options.addOption( dbFile );
        Option queriesFile   = Option.builder("f")
                .required(true) // TODO: It will be optional in the future if we add a shell
                .longOpt("file")
                .desc( "A file with the source PGQL queries ended with a \"?\" character." )
                .hasArg()
                .build();
        options.addOption( queriesFile );



        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            String databaseFilePath = null;
            String queriesFilePath = null;

            Client client = new Client();
            client.setShowAlgebra ( line.hasOption("a"));

            if (line.hasOption( "db")) {
                databaseFilePath = line.getOptionValue("db");
            }
            if (line.hasOption("f") ) {
                queriesFilePath = line.getOptionValue("f");
            }
            if (line.hasOption("c")) {
                if (databaseFilePath == null) {
                    System.err.println("The Sparksee database file to create must be specified!");
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("SparkseePGQL.Client", options);
                    System.exit(1);
                    return;
                }
               if ( !client.createSampleDatabase(databaseFilePath) ) {
                   System.err.println("The sample Sparksee database could not be created!");
                   System.exit(1);
                   return;
               }
            } else if (databaseFilePath != null) {
                if ( !client.openDatabase(databaseFilePath) ) {
                    System.err.println("The sample Sparksee database could not be created!");
                    System.exit(1);
                    return;
                }
            }

            assert(queriesFilePath != null);
            client.processScript( queriesFilePath );
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkseePGQL.Client", options);
            System.exit(1);
            return;
        }
    }
}

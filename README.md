# SparkseePGQL
PGQL to Sparksee Query Algebra

### What is this repository for? ###

A library to translate a PGQL query to the SparkseeQueryAlgebra.

### Current status ###

It's usable for some queries, but the full PGQL language is not supported yet.
Some parts of the language are still missing, like PATHS, indgree/outdegree, ...

The result Sparksee Algebra Query may use operators only available in a not yet published Sparksee release.

### How do I get set up? ###

You will need the [Oracle/PGQL-LANG](https://github.com/oracle/pgql-lang).

Then you can build the package with:
```
mvn assembly:assembly
```

The method *"public String ProcessPGQLQuery(String query)"* from the class *"SparkseePGQL"* is what you need to translate a PGQL query to the Sparksee Query Algebra.


You can also use the client command line application to translate a PGQL query like this:
```
java -cp target/SparkseePGQL-1.0-SNAPSHOT-jar-with-dependencies.jar com.sparsity.SparkseePGQL.Client -a -f src/test/resources/test.pgql
```

Or if you also want to create a small sample database and run the query in sparksee:
```
java -cp target/SparkseePGQL-1.0-SNAPSHOT-jar-with-dependencies.jar com.sparsity.SparkseePGQL.Client -a -c -db test.gdb -f src/test/resources/test.pgql
```

The test.pgql file contains PGQL queries ended by a "?" character.



package org.linkeddatafragments.datasource;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A SPARQL data source of Basic Linked Data Fragments.
 *
 * @author Eugene Siow
 */
public class SparqlDataSource extends DataSource {
	
	static Logger log = Logger.getLogger(SparqlDataSource.class.getName());

    private final String endpoint;

    /**
     * Creates a new SparqlDataSource.
     *
     * @param title title of the datasource
     * @param description datasource description
     * @param endpoint SPARQL endpoint
     */
    public SparqlDataSource(String title, String description, String endpoint) {
        super(title, description);
        this.endpoint = endpoint;
    }

    @Override
    public TriplePatternFragment getFragment(Resource subject, Property predicate, RDFNode object, final long offset, final long limit) {
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit");
        }
        
        final String subjectPart = subject == null ? "?s " : "<" + subject.getURI() +"> ";
        final String predicatePart = predicate == null ? "?p " : "<" + predicate.getURI() +"> ";
        final String objectPart = object == null ? 
        		"?o " : 
        			object.isLiteral() ? "\""+object.asLiteral()+"\" " : "<" + object.asResource().getURI() +"> ";
        String sparqlPattern = "{" + subjectPart + predicatePart + objectPart + "}";
        
		String queryString = 
			"	CONSTRUCT\n\r"+
			sparqlPattern +"\n\r"+
			"	WHERE\n\r"+
			sparqlPattern +"\n\r"+
			"	LIMIT "+limit+" OFFSET "+offset;
		String countString = "SELECT (count(*) as ?count) WHERE\n\r" + 
			sparqlPattern + " LIMIT " + limit + " OFFSET " + offset;

		long startTime = System.currentTimeMillis();
		QueryExecution tripleQuery = QueryExecutionFactory.sparqlService(endpoint, queryString);
		final Model results = tripleQuery.execConstruct();
		//results.write(System.out, "TURTLE");
		long totalTime = System.currentTimeMillis() - startTime;
		log.warn("CONSTRUCT_TIME;" + totalTime + ";" + sparqlPattern + ";" + offset);
		
		startTime = System.currentTimeMillis();
		QueryExecution countQuery = QueryExecutionFactory.sparqlService(endpoint, countString);
		ResultSet countResults = countQuery.execSelect();
		totalTime = System.currentTimeMillis() - startTime;
		log.warn("COUNT_TIME;" + totalTime + ";" + sparqlPattern + ";" + offset);
		final long totalSize = countResults.next().getLiteral("count").getLong();

//        final IteratorTripleID result = datasource.getTriples().search(new TripleID(subjectId, predicateId, objectId));
//        // estimates can be wrong; ensure 0 is returned if and only if there are no results
//        final long totalSize = result.hasNext() ? Math.max(result.estimatedNumResults(), 1) : 0;
//
        // create the fragment
        return new TriplePatternFragment() {
        	@Override
			public Model getTriples() { return results; }
			
			@Override
			public long getTotalSize() { return totalSize; }
        };
    }
}

package org.linkeddatafragments.datasource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;

/**
 * An TDB data source of Basic Linked Data Fragments.
 * Does not handle bnode (anon nodes) well.
 *
 * @author Eugene Siow
 */
public class TdbDataSource extends DataSource {

    private final Model datasource;
    private Map<String,Long> sizeCache;
    static Logger log = Logger.getLogger(SparqlDataSource.class.getName());

    /**
     * Creates a new HdtDataSource.
     *
     * @param title title of the datasource
     * @param description datasource description
     * @param hdtFile the HDT datafile
     * @throws IOException if the file cannot be loaded
     */
    public TdbDataSource(String title, String description, String TdbDirectory) throws IOException {
        super(title, description);
//        System.out.println(TdbDirectory);
        datasource = TDBFactory.createDataset(TdbDirectory).getDefaultModel();
        sizeCache = new HashMap<String,Long>();
    }

    @Override
    public TriplePatternFragment getFragment(Resource subject, Property predicate, RDFNode object, final long offset, final long limit) {
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit");
        }
        
        long startTime = System.currentTimeMillis();
        
        final Model triples = ModelFactory.createDefaultModel();
        
        Resource subjectNode = subject == null ? null : subject.asResource();
        Property predicateNode = predicate == null ? null : predicate;
        Resource objectNode = object == null ? null : object.asResource(); 
        
        String sidentifier = subject == null ? "null" : subject.getURI();
        String pidentifier = predicate == null ? "null" : predicate.getURI();
        String oidentifier = object == null ? "null" : 
        	object.isLiteral() ? object.asLiteral().getString() : object.asResource().getURI();
        	
        String identifier = sidentifier + ":" + pidentifier + ":" + oidentifier;
        if(subject!=null && subject.isAnon()) {
        	System.out.println("hooray");
        	System.out.println(sidentifier);
        }
        
//        System.out.println(identifier);
 
        final StmtIterator result = datasource.listStatements(subjectNode, predicateNode, objectNode);
        
        long size = 0;
        // try to jump directly to the offset
        boolean atOffset;
        for (int i = 0; !(atOffset = i == offset) && result.hasNext(); i++) {
            result.next();
            size++;
        }
        // add `limit` triples to the result model
        if (atOffset) {
            for (int i = 0; i < limit && result.hasNext(); i++) {
            	Statement st = result.next();
            	triples.add(st);
            	size++;
            }
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
		log.warn("CONSTRUCT_TIME;" + totalTime + ";" + identifier + ";" + offset);

		startTime = System.currentTimeMillis();
        if(sizeCache.containsKey(identifier)) {
        	size = sizeCache.get(identifier);
        } else {
	        while(result.hasNext()) {
	        	try {
		        	result.next();
		        	size++;
	        	} catch(Exception e) {
	        		e.printStackTrace();
	        	}
	        }
	        sizeCache.put(identifier, size);
        }
        
        final long totalSize = size;
        totalTime = System.currentTimeMillis() - startTime;
		log.warn("COUNT_TIME;" + totalTime + ";" + identifier + ";" + offset);

        // create the fragment
        return new TriplePatternFragment() {
            @Override
            public Model getTriples() {
                return triples;
            }

            @Override
            public long getTotalSize() {
                return totalSize;
            }
        };
    }
}

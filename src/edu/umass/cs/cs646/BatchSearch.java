package edu.umass.cs.cs646;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModel;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import static org.lemurproject.galago.core.tools.apps.BatchSearch.logger;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author Hamed Zamani (zamani@cs.umass.edu)
 */
public class BatchSearch {
    
    public static void main(String[] args) throws Exception{
        String indexPath = "path-to-index";
        String outputFileName = "path-to-output-file";
        new BatchSearch().retrieve(indexPath, outputFileName);
    }

    public void retrieve(String indexPath, String outputFileName) throws Exception {
        int requested = 1000; // number of documents to retrieve
        boolean append = false;
        boolean queryExpansion = true;
        // open index
        Retrieval retrieval = RetrievalFactory.instance(indexPath, Parameters.create());
        
        // load queries
        List <Parameters> queries = new ArrayList <> ();
        queries.add(Parameters.parseString(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", "301", "International Organized Crime")));
        
        // open output file
        ResultWriter resultWriter = new ResultWriter(outputFileName, append);

        // for each query, run it, get the results, print in TREC format
        for (Parameters query : queries) {
            String queryNumber = query.getString("number");
            String queryText = query.getString("text");
            queryText = queryText.toLowerCase(); // option to fold query cases -- note that some parameters may require upper case
            
            logger.info("Processing query #" + queryNumber + ": " + queryText);
            
            query.set("requested", requested);

            Node root = StructuredQuery.parse(queryText);
            Node transformed = retrieval.transformQuery(root, query);
            
            // Query Expansion
            if (queryExpansion){
                // This query expansion technique can be replaced by other approaches.
                ExpansionModel qe = new org.lemurproject.galago.core.retrieval.prf.RelevanceModel3(retrieval);
//                ExpansionModel qe = new MixtureFeedbackModel(retrieval);

                try{
                    query.set("fbOrigWeight", 0.5);
                    query.set("fbTerm", 100.0);
                    Node expandedQuery = qe.expand(root.clone(), query.clone());  
                    transformed = retrieval.transformQuery(expandedQuery, query);
                } catch (Exception ex){
                    ex.printStackTrace();
                }
            }
            
//            System.err.println(transformed.toPrettyString()); // This can be used to print the final query in the Galago language.
            // run query
            List<ScoredDocument> results = retrieval.executeQuery(transformed, query).scoredDocuments;
            
            // print results
            resultWriter.write(queryNumber, results);
        }
        resultWriter.close();
    }
}

package edu.umass.cs.cs646;

import com.univocity.parsers.tsv.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Reader;
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
//        String indexPath = "/home/pxyu/Documents/github/646-hw2/robust04-complete-index/";
//        String outputFileName = "/home/pxyu/Documents/github/646-hw2/search_results/mm.txt";
//        String queryFileName = "/home/pxyu/Documents/github/646-hw2/query.titles.tsv";
        String indexPath = "C:\\Users\\Puxuan Yu\\Documents\\GitHub\\646-hw2\\robust04-complete-index\\";
        String outputFileName = "C:\\Users\\Puxuan Yu\\Documents\\GitHub\\646-hw2\\search_results\\mm.txt";
        String queryFileName = "C:\\Users\\Puxuan Yu\\Documents\\GitHub\\646-hw2\\query.titles.tsv";
        new BatchSearch().retrieve(indexPath, outputFileName, queryFileName);
    }

    public void retrieve(String indexPath, String outputFileName, String queryFileName) throws Exception {
        int requested = 1000; // number of documents to retrieve
        boolean append = false;
        boolean queryExpansion = true;
        // open index
        Retrieval retrieval = RetrievalFactory.instance(indexPath, Parameters.create());

        // load queries
        TsvParser parser = new TsvParser(new TsvParserSettings());
        List<String[]> queriesFile = parser.parseAll(getFile(queryFileName));
        List <Parameters> queries = new ArrayList <> ();

        for (String[] line: queriesFile){
            queries.add(Parameters.parseString(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", line[0], line[1])));
        }

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
                //ExpansionModel qe = new org.lemurproject.galago.core.retrieval.prf.RelevanceModel3(retrieval);
               ExpansionModel qe = new MixtureFeedbackModel(retrieval);

                try{
                    query.set("fbOrigWeight", 0.5);
                    query.set("fbDocs", 10.0);
                    query.set("fbTerm", 50.0);
                    query.set("lambda", 0.5);
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

    public static Reader getFile(String relativePath) {
        try {
            InputStreamReader reader = new InputStreamReader(new FileInputStream(relativePath));
            return reader;
        } catch (FileNotFoundException e) {
        }
        return null;
    }
}

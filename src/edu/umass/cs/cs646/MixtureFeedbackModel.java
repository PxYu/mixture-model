package edu.umass.cs.cs646;

import java.io.IOException;
import java.util.*;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.prf.ExpansionModel;
import org.lemurproject.galago.core.retrieval.prf.WeightedTerm;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author Hamed Zamani (zamani@cs.umass.edu)
 */
public class MixtureFeedbackModel implements ExpansionModel{
    protected Retrieval retrieval;
    int defaultFbDocs, defaultFbTerms;
    double defaultFbOrigWeight;
    Set <String> exclusionTerms;
    Stemmer stemmer;
    
    public MixtureFeedbackModel (Retrieval r) throws IOException{
        retrieval = r;
        defaultFbDocs = (int) Math.round(r.getGlobalParameters().get("fbDocs", 10.0));
        defaultFbTerms = (int) Math.round(r.getGlobalParameters().get("fbTerm", 100.0));
        defaultFbOrigWeight = r.getGlobalParameters().get("fbOrigWeight", 0.2);
        exclusionTerms = WordLists.getWordList(r.getGlobalParameters().get("rmstopwords", "rmstop"));
        Parameters gblParms = r.getGlobalParameters();
        this.stemmer = FeedbackData.getStemmer(gblParms, retrieval);
    }
    
    public List<ScoredDocument> collectInitialResults(Node transformed, Parameters fbParams) throws Exception {
        Results results = retrieval.executeQuery(transformed, fbParams);
        List<ScoredDocument> res = results.scoredDocuments;
        if (res.isEmpty())
            throw new Exception("No feedback documents found!");
        return res;
    }
    
    public Node generateExpansionQuery(List<WeightedTerm> weightedTerms, int fbTerms) throws IOException, Exception {
        Node expNode = new Node("combine");
        System.err.println("Feedback Terms:");
        for (int i = 0; i < Math.min(weightedTerms.size(), fbTerms); i++) {
          Node expChild = new Node("text", weightedTerms.get(i).getTerm());
          expNode.addChild(expChild);
          expNode.getNodeParameters().set("" + i, weightedTerms.get(i).getWeight());
        }
        return expNode;
    }
    
    public int getFbDocCount (Node root, Parameters queryParameters) throws Exception{
        int fbDocs = (int)Math.round(root.getNodeParameters().get("fbDocs", queryParameters.get("fbDocs", (double) defaultFbDocs)));
        if (fbDocs <= 0)
            throw new Exception ("Invalid number of feedback documents!");
        return fbDocs;
    }
    
    public int getFbTermCount (Node root, Parameters queryParameters) throws Exception{
        int fbTerms = (int) Math.round(root.getNodeParameters().get("fbTerm", queryParameters.get("fbTerm", (double) defaultFbTerms)));
        if (fbTerms <= 0)
            throw new Exception ("Invalid number of feedback terms!");
        return fbTerms;
    }
    
    public Node interpolate (Node root, Node expandedQuery, Parameters queryParameters) throws Exception{
        queryParameters.set("defaultFbOrigWeight", defaultFbOrigWeight);
        queryParameters.set("fbOrigWeight", queryParameters.get("fbOrigWeight", defaultFbOrigWeight));
        return linearInterpolation(root, expandedQuery, queryParameters);
    }
    
    public Node linearInterpolation (Node root, Node expNode, Parameters parameters) throws Exception{
        double defaultFbOrigWeight = parameters.get("defaultFbOrigWeight", -1.0);
        if (defaultFbOrigWeight < 0)
            throw new Exception ("There is not defaultFbOrigWeight parameter value");
        double fbOrigWeight = parameters.get("fbOrigWeight", defaultFbOrigWeight);
        if (fbOrigWeight == 1.0) {
            return root;
        }
        Node result = new Node("combine");
        result.addChild(root);
        result.addChild(expNode);
        result.getNodeParameters().set("0", fbOrigWeight);
        result.getNodeParameters().set("1", 1.0 - fbOrigWeight);
        return result;
    }
    
    public Parameters getFbParameters (Node root, Parameters queryParameters) throws Exception{
        Parameters fbParams = Parameters.create();
        fbParams.set("requested", getFbDocCount(root, queryParameters));
        fbParams.set("passageQuery", false);
        fbParams.set("extentQuery", false);
        fbParams.setBackoff(queryParameters);
        return fbParams;
    }

    @Override
    public Node expand(Node root, Parameters queryParameters) throws Exception {
        int fbTerms = getFbTermCount(root, queryParameters); 
        // transform query to ensure it will run
        Parameters fbParams = getFbParameters(root, queryParameters);
        Node transformed = retrieval.transformQuery(root.clone(), fbParams);
        
        // get some initial results
        List<ScoredDocument> initialResults = collectInitialResults(transformed, fbParams);
   
        
        // extract grams from results
        Set<String> queryTerms = getTerms(stemmer, StructuredQuery.findQueryTerms(transformed));
        FeedbackData feedbackData = new FeedbackData(retrieval, exclusionTerms, initialResults, fbParams);
        List <WeightedTerm> weightedTerms = computeWeights(feedbackData, fbParams, queryParameters, queryTerms);
        Collections.sort(weightedTerms);
        Node expNode = generateExpansionQuery(weightedTerms, fbTerms);
        
        return interpolate(root, expNode, queryParameters);
    }
    
    public static Set<String> getTerms(Stemmer stemmer, Set<String> terms) {
      if (stemmer == null)
          return terms;

      Set<String> stems = new HashSet<String>(terms.size());
      for (String t : terms) {
        String s = stemmer.stem(t);
        stems.add(s);
      }
      return stems;
    }
    
    public List <WeightedTerm> computeWeights (FeedbackData feedbackData, Parameters fbParam, Parameters queryParameters, Set <String> queryTerms) throws Exception{
//        throw new Exception ("This should be implemented! This method outputs a list of terms with weights.");
        Retrieval retrieval = this.retrieval;
        Map<String, Map<ScoredDocument, Integer>> termCounts = feedbackData.termCounts;

        Node fieldNode = StructuredQuery.parse("#lengths:text:part=lengths()");
        FieldStatistics fieldStats = retrieval.getCollectionStatistics(fieldNode);
        long corpusLength = fieldStats.collectionLength;
        double pwc = 0;

        retrieval.close();
        int numTerms = termCounts.size();

        //non-static maps
        Map<String, Double> weights = new HashMap<>();
        Map<String, Double> nextWeights = new HashMap<>();
        Map<String, Double> ps = new HashMap<>();

        //static maps
        Map<String, Integer> termFreqs = new HashMap<>();
        Map<String, Double> pwcs = new HashMap<>();

        // initial round
        for (String term: termCounts.keySet()){
            // initiate weights map
            double est = 1 / numTerms;
            weights.put(term, est);

            // record termFreqs map
            int termFreq = 0;
            Map<ScoredDocument, Integer> tmap = termCounts.get(term);
            for (Map.Entry<ScoredDocument, Integer> entry : tmap.entrySet()) {
                termFreq += entry.getValue();
            }
            termFreqs.put(term, termFreq);

            // record pwcs map
            Node termNode = StructuredQuery.parse( "#text:" + term + ":part=field.text()");
            termNode.getNodeParameters().set( "queryType", "count" );
            NodeStatistics termStats = retrieval.getNodeStatistics( termNode );
            long corpusTF = termStats.nodeFrequency; // Get the total frequency of the term in the text field
            pwc = corpusTF / corpusLength;
            pwcs.put(term, pwc);

            // initial ps map
            double p = est / est + pwc;
            ps.put(term, p);

            // compute nextWeights map
            double ttl = 0.0;
            for (Map.Entry<String, Double> entry: ps.entrySet()){
                ttl += termFreqs.get(entry.getKey()) * entry.getValue();
            }
            double newWeight = (termFreq * p) / ttl;
            nextWeights.put(term, newWeight);
        }

        while (!convergence(weights, nextWeights)) {

            weights = nextWeights;

            for (String term: termCounts.keySet()){
                // update ps
                double weight = weights.get(term);
                double p = weight / (weight + pwcs.get(term));
                ps.put(term, p);
            }

            for (String term: termCounts.keySet()){
                // update newWeights
                double ttl = 0.0;
                int termFreq = termFreqs.get(term);
                for (Map.Entry<String, Double> entry: ps.entrySet()){
                    ttl += termFreqs.get(entry.getKey()) * entry.getValue();
                }
                double newWeight = (termFreq * ps.get(term)) / ttl;
                nextWeights.put(term, newWeight);
            }
        }

        return convert(nextWeights);
    }

    public boolean convergence(Map<String, Double> a, Map<String, Double> b) {
        for (Map.Entry<String, Double> entry: a.entrySet()){
            if (Math.abs(b.get(entry.getKey())-entry.getValue()) < 0.0001){
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    public List <WeightedTerm> convert(Map<String, Double> weights) {
        List <WeightedTerm> res = new LinkedList<>();
        for (String t: weights.keySet()){
            WeightedTerm wt = new WeightedUnigram(t, weights.get(t));
            res.add(wt);
        }
        return res;
    }
}

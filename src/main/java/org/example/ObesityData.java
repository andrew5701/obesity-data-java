package org.example;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.util.FileManager;

public class ObesityData {

    public static void main(String[] args) {
        Model modelObesity = FileManager.get().loadModel("src/main/resources/ObesityData.ttl", null, "TTL");

        // Query 1: Find the number of individuals who are obese by ethnicity, gender, or percentage of family income above the federal poverty level (%)
        System.out.println("\nQuery 1: Find the number of individuals who are obese by ethnicity, gender, or percentage of family income above the federal poverty level (%)\n");
        String selectQueryObesityByGroup =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "SELECT ?group (COUNT(?individual) AS ?count) " +
                        "WHERE { " +
                        "  ?individual ds:estimate ?bmi; " +
                        "              ds:stub_label ?group. " +
                        "  FILTER(xsd:decimal(?bmi) > 24.9) " +
                        "} " +
                        "GROUP BY ?group ";
        executeSelectQueryForGroups(modelObesity, selectQueryObesityByGroup);

        // Query 2: Find the number of individuals who are obese by year
        System.out.println("\nQuery 2: Find the number of individuals who are obese by year\n");
        String selectQueryObesityByYear =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "SELECT ?year (COUNT(?individual) AS ?count) " +
                        "WHERE { " +
                        "  ?individual ds:estimate ?bmi; " +
                        "              ds:year ?year. " +
                        "  FILTER(xsd:decimal(?bmi) > 24.9) " +
                        "} " +
                        "GROUP BY ?year " +
                        "ORDER BY ?year";
        executeSelectQuery(modelObesity, selectQueryObesityByYear);

        // Query 3: Find the number of individuals who are underweight by year
        System.out.println("\nQuery 3: Find the number of individuals who are underweight by year\n");
        String selectQueryUnderweightByYear =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "SELECT ?year (COUNT(?individual) AS ?count) " +
                        "WHERE { " +
                        "  ?individual ds:estimate ?bmi; " +
                        "              ds:year ?year. " +
                        "  FILTER(xsd:decimal(?bmi) < 18.5) " +
                        "} " +
                        "GROUP BY ?year " +
                        "ORDER BY ?year";
        executeSelectQuery(modelObesity, selectQueryUnderweightByYear);

        // Query 4: Find the number of individuals who are within normal weight range by year
        System.out.println("\nQuery 4: Find the number of individuals who are within normal weight by year\n");
        String selectQueryNormalWeightByYear =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "SELECT ?year (COUNT(?individual) AS ?count) " +
                        "WHERE { " +
                        "  ?individual ds:estimate ?bmi; " +
                        "              ds:year ?year. " +
                        "  FILTER(xsd:decimal(?bmi) >= 18.5 && xsd:decimal(?bmi) <= 24.9) " +
                        "} " +
                        "GROUP BY ?year " +
                        "ORDER BY ?year";
        executeSelectQuery(modelObesity, selectQueryNormalWeightByYear);
        // Query 5: Find the group with the highest number of overweight individuals
        System.out.println("\nQuery 5: Find the group with the highest number of overweight individuals\n");
        String selectQueryHighestOverweight =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "SELECT ?group (COUNT(?individual) AS ?count) " +
                        "WHERE { " +
                        "  ?individual ds:estimate ?bmi; " +
                        "              ds:stub_label ?group. " +
                        "  FILTER(xsd:decimal(?bmi) > 24.9) " +
                        "} " +
                        "GROUP BY ?group " +
                        "ORDER BY DESC(?count) " +
                        "LIMIT 1";
        executeSelectQueryForSingleGroup(modelObesity, selectQueryHighestOverweight);

        // Query 6: Ask whether there are any individuals who are underweight and are 45-54 years old
        System.out.println("\nQuery 6: Ask whether there are any individuals who are underweight and are 45-54 years old\n");
        String askQueryUnderweight =
                "PREFIX ds: <https://data.cdc.gov/resource/_3nzu-udr9/> " +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                        "ASK { " +
                        "  ?individual ds:age \"45-54 years\" ; " +
                        "              ds:estimate ?bmi . " +
                        "  FILTER(xsd:decimal(?bmi) < 18.5) " +
                        "}";
        executeAskQuery(modelObesity, askQueryUnderweight);
    }

    private static void executeSelectQuery(Model model, String queryString) {
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            System.out.println("Results:");
            while (results.hasNext()) {
                QuerySolution sol = results.nextSolution();
                try {
                    Literal year = sol.getLiteral("year");
                    Literal count = sol.getLiteral("count");
                    System.out.println("Year: " + year.getString() + ", Count: " + count.getInt());
                } catch (NullPointerException e) {
                    System.out.println("Some expected data was not found in one of the rows.");
                }
            }
        } catch (Exception e) {
            System.err.println("Error executing SELECT query: " + e.getMessage());
            e.printStackTrace();
        }
    }
    private static void executeSelectQueryForGroups(Model model, String queryString) {
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            System.out.println("Results:");
            while (results.hasNext()) {
                QuerySolution sol = results.nextSolution();
                try {
                    String group = sol.getLiteral("group").getString();
                    int count = sol.getLiteral("count").getInt();
                    System.out.println("Group: " + group + ", Count: " + count);
                } catch (NullPointerException e) {
                    System.out.println("Incomplete data for some groups.");
                }
            }
        } catch (Exception e) {
            System.err.println("Error executing SELECT query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executeSelectQueryForSingleGroup(Model model, String queryString) {
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            if (results.hasNext()) {
                QuerySolution sol = results.nextSolution();
                Literal groupLiteral = sol.getLiteral("group");
                Literal countLiteral = sol.getLiteral("count");

                String group = (groupLiteral != null) ? groupLiteral.getString() : "Unknown group";
                int count = (countLiteral != null) ? countLiteral.getInt() : 0;

                System.out.println("Group: " + group + ", Count: " + count);
            } else {
                System.out.println("No results found.");
            }
        } catch (Exception e) {
            System.err.println("Error executing SELECT query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void executeAskQuery(Model model, String queryString) {
        Query query = QueryFactory.create(queryString);
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            boolean result = qexec.execAsk();
            System.out.println("ASK Query Result: " + result);
        } catch (Exception e) {
            System.err.println("Error executing ASK query: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
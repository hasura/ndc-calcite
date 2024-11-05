package com.hasura;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.graphql.GraphQLRules;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * CalciteModelPlanner is responsible for displaying query plans with optimization rules applied.
 */
public class CalciteModelPlanner {
        /**
         * Display the query plan for the given SQL statement using Calcite framework.
         *
         * @param modelPath the path to the model JSON file
         * @param sql the SQL statement for which to generate the query plan
         * @throws Exception if an error occurs during query plan generation
         */
        public static void displayQueryPlan(String modelPath, String sql) throws Exception {
                Properties info = new Properties();
                info.put("model", modelPath);
                info.setProperty("caseSensitive", "true");
                info.setProperty("unquotedCasing", "UNCHANGED");
                info.setProperty("quotedCasing", "UNCHANGED");

                Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
                CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
                SchemaPlus rootSchema = calciteConnection.getRootSchema();

                // Create rules list
                List<RelOptRule> rules = new ArrayList<>();

                // Add GraphQL rules
                rules.add(GraphQLRules.TO_ENUMERABLE);
                rules.addAll(GraphQLRules.RULES);

                // Add core conversion rules
                rules.add(CoreRules.FILTER_INTO_JOIN);
                rules.add(CoreRules.JOIN_CONDITION_PUSH);
                rules.add(CoreRules.PROJECT_MERGE);
                rules.add(CoreRules.PROJECT_SET_OP_TRANSPOSE);

                // Add Enumerable conversion rules
                rules.add(EnumerableRules.ENUMERABLE_PROJECT_RULE);
                rules.add(EnumerableRules.ENUMERABLE_FILTER_RULE);
                rules.add(EnumerableRules.ENUMERABLE_JOIN_RULE);
                rules.add(EnumerableRules.ENUMERABLE_SORT_RULE);
                rules.add(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
                rules.add(EnumerableRules.ENUMERABLE_VALUES_RULE);

                // Create RuleSet
                RuleSet ruleSet = RuleSets.ofList(rules);

                // Create program builder for transformation rules
                HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
                hepProgramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
                rules.forEach(hepProgramBuilder::addRuleInstance);

                // Set the SQL parser configuration
                SqlParser.Config parserConfig = SqlParser.config()
                        .withCaseSensitive(true)  // For distinguishing between "name" and "Name"
                        .withUnquotedCasing(Casing.UNCHANGED)  // Keep original case for unquoted identifiers
                        .withQuotedCasing(Casing.UNCHANGED)    // Keep original case for quoted identifiers
                        .withConformance(SqlConformanceEnum.LENIENT);

                // Create planner configuration
                FrameworkConfig config = Frameworks.newConfigBuilder()
                        .defaultSchema(rootSchema)
                        .parserConfig(parserConfig)
                        .programs(Programs.sequence(
                                Programs.ofRules(rules),
                                Programs.ofRules(GraphQLRules.PROJECT_RULE),
                                Programs.ofRules(GraphQLRules.TO_ENUMERABLE)
                        ))
                        .build();

                try {
                        // Parse and validate SQL
                        Planner planner = Frameworks.getPlanner(config);
                        SqlNode parsed = planner.parse(sql);
                        SqlNode validated = planner.validate(parsed);
                        RelRoot root = planner.rel(validated);
                        RelNode relNode = root.rel;

                        System.out.println("Initial plan:");
                        System.out.println(RelOptUtil.toString(relNode));

                        // Get the planner and add rules
                        RelOptPlanner planner2 = relNode.getCluster().getPlanner();
                        rules.forEach(planner2::addRule);

                        System.out.println("\nApplying rules...");

                        try {
                                // Log initial state
                                System.out.println("\nInitial RelNode traits: " + relNode.getTraitSet());
                                System.out.println("Initial RelNode digest: " + relNode.getDigest());

                                // Try optimization steps one at a time
                                RelNode currentNode = relNode;
                                RelTraitSet targetTraits = currentNode.getTraitSet()
                                        .replace(EnumerableConvention.INSTANCE);

                                System.out.println("Target traits: " + targetTraits);

                                try {
                                        // Attempt to change traits
                                        currentNode = planner2.changeTraits(currentNode, targetTraits);
                                        System.out.println("\nAfter trait change:");
                                        System.out.println(RelOptUtil.toString(currentNode));
                                } catch (Exception e) {
                                        System.out.println("Error during trait change: " + e.getMessage());
                                }

                                try {
                                        // Set as root and optimize
                                        System.out.println("\nSetting root node...");
                                        planner2.setRoot(currentNode);
                                        System.out.println("Root set successfully");
                                } catch (Exception e) {
                                        System.out.println("Error setting root: " + e.getMessage());
                                        throw e;
                                }

                                try {
                                        // Find best expression
                                        System.out.println("\nFinding best expression...");
                                        RelNode optimized = planner2.findBestExp();
                                        System.out.println("\nOptimized plan:");
                                        System.out.println(RelOptUtil.toString(optimized));

                                        // Check for GraphQLProject
                                        String plan = RelOptUtil.toString(optimized);
                                        if (plan.contains("GraphQLProject")) {
                                                System.out.println("\nSuccess: Plan contains GraphQLProject");
                                        } else {
                                                System.out.println("\nWarning: Plan does not contain GraphQLProject");
                                                System.out.println("Available rules:");
                                                planner2.getRules().forEach(rule ->
                                                        System.out.println(" - " + rule.toString())
                                                );
                                        }
                                } catch (Exception e) {
                                        System.out.println("Error finding best expression: " + e.getMessage());
                                        throw e;
                                }

                        } catch (Exception e) {
                                System.out.println("Error during optimization:");
                                e.printStackTrace();

                                // Print planner state
                                System.out.println("\nPlanner rules:");
                                planner2.getRules().forEach(rule ->
                                        System.out.println(" - " + rule.toString())
                                );

                                System.out.println("\nCurrent RelNode:");
                                System.out.println(RelOptUtil.toString(relNode));
                        }

                } catch (Exception e) {
                        System.out.println("Error during initialization:");
                        e.printStackTrace();
                }
        }
}
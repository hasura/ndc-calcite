package com.hasura.splunk;

/**
 * Marker interface for integration tests that require a live Splunk connection.
 * These tests will be skipped if proper Splunk credentials are not available.
 */
public interface IntegrationTest {
}
use ndc_models::{
    Capabilities, CapabilitiesResponse, LeafCapability, MutationCapabilities,
    NestedFieldCapabilities, QueryCapabilities, RelationshipCapabilities,
};

/// Calculates the capabilities of the Calcite system.
///
/// The `calcite_capabilities` function returns a `CapabilitiesResponse` struct
/// that represents the capabilities of the Calcite system. The `CapabilitiesResponse`
/// contains the version of the Calcite system and various capabilities in different
/// aspects such as query, mutation, and relationships.
///
/// # Example
///
/// ```rust
/// use some_library::calcite_capabilities;
///
/// let capabilities = calcite_capabilities();
/// println!("{:?}", capabilities);
/// ```
///
/// # Returns
///
/// - `CapabilitiesResponse`: A struct representing the capabilities of the Calcite system.
// ANCHOR: calcite_capabilities
pub fn calcite_capabilities() -> CapabilitiesResponse {
    CapabilitiesResponse {
        version: "0.1.4".into(),
        capabilities: Capabilities {
            query: QueryCapabilities {
                aggregates: Some(LeafCapability {}),
                variables: Some(LeafCapability {}),
                explain: None,
                nested_fields: NestedFieldCapabilities {
                    filter_by: None,
                    order_by: None,
                    aggregates: None,
                },
            },
            mutation: MutationCapabilities {
                transactional: None,
                explain: None,
            },
            relationships: Some(RelationshipCapabilities {
                order_by_aggregate: None,
                relation_comparisons: None,
            }),
        },
    }
}
// ANCHOR_END: calcite_capabilities

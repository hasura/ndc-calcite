use ndc_models::{
    Capabilities, CapabilitiesResponse, LeafCapability, MutationCapabilities,
    NestedFieldCapabilities, QueryCapabilities, RelationshipCapabilities,
};

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

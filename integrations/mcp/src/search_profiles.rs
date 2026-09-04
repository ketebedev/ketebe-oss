use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct ListSearchProfilesParams {
    pub collection: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct DescribeSearchProfileParams {
    pub collection: String,
    pub profile: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SearchProfileView {
    pub name: String,
    pub version: u64,
    pub pinned_id: String,
    pub execution: String,
    pub dense_candidates: Option<usize>,
    pub sparse_candidates: Option<usize>,
    pub rrf_k: u32,
    pub final_top_k: usize,
    pub timeout_ms: Option<u64>,
    pub rerank: Option<SearchProfileRerankView>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct SearchProfileRerankView {
    pub profile: String,
    pub top_n: usize,
    pub text_fields: Vec<Vec<String>>,
    pub include_metadata: bool,
    pub failure_policy: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListSearchProfilesOutput {
    pub profiles: Vec<SearchProfileView>,
}

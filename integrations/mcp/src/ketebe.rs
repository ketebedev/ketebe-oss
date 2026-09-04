use crate::error::McpError;
use crate::fusion::{FusedSearchOutput, FusedSearchParams, ServerRerankParams, fuse_results};
use crate::multi_search::{
    CollectionSearchResult, CollectionSearchStatus, ProvenancedSearchHit, SearchManyOutput,
    SearchManyParams,
};
use crate::observability::ObservedHttpClient;
use crate::retrieval::{
    AgentRecordId, FetchRecordsRequest, FetchRecordsResponse, GetRecordsOutput, RecordFetchError,
    RecordView,
};
use crate::search::{
    SearchError, SearchMode, SearchOutput, SearchParams, SearchRequest, SearchResponse,
};
use crate::search_profiles::SearchProfileView;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct KetebeApi {
    pub(crate) base_url: String,
    client: ketebe_sdk::Client,
    pub(crate) http: ObservedHttpClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationProbeError {
    Unauthenticated,
    Forbidden,
    Unavailable,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
}

#[derive(Debug, Serialize)]
struct QueryEmbeddingRequest<'a> {
    text: &'a str,
}

#[derive(Debug, Deserialize)]
struct QueryEmbeddingResponse {
    vector: Vec<f32>,
}

impl KetebeApi {
    pub fn new(base_url: impl Into<String>) -> Result<Self, ketebe_sdk::Error> {
        let base_url = base_url.into();
        let client = ketebe_sdk::Client::new(ketebe_sdk::ClientConfig::new(base_url.clone()))?;
        Ok(Self {
            base_url,
            client,
            http: ObservedHttpClient::new(),
        })
    }

    pub(crate) fn client_for(
        &self,
        bearer_token: Option<&str>,
    ) -> Result<ketebe_sdk::Client, ketebe_sdk::Error> {
        let config = ketebe_sdk::ClientConfig::new(self.base_url.clone());
        let config = match bearer_token {
            Some(token) => config.with_bearer_token(token),
            None => config,
        };
        ketebe_sdk::Client::new(config)
    }

    pub async fn probe(&self) -> Result<(), ketebe_sdk::Error> {
        self.client.health().await
    }

    pub async fn list_collections(
        &self,
        bearer_token: Option<&str>,
    ) -> Result<Vec<ketebe_sdk::Collection>, ketebe_sdk::Error> {
        self.client_for(bearer_token)?.list_collections().await
    }

    pub async fn get_collection(
        &self,
        collection: &str,
        bearer_token: Option<&str>,
    ) -> Result<ketebe_sdk::Collection, ketebe_sdk::Error> {
        self.client_for(bearer_token)?
            .get_collection(collection)
            .await
    }

    pub async fn list_search_profiles(
        &self,
        collection: &str,
        bearer_token: Option<&str>,
    ) -> Result<Vec<SearchProfileView>, String> {
        let url = format!(
            "{}/v1/collections/{}/search-profiles",
            self.base_url.trim_end_matches('/'),
            collection
        );
        self.profile_request(self.http.get(url), bearer_token).await
    }

    pub async fn get_search_profile(
        &self,
        collection: &str,
        profile: &str,
        bearer_token: Option<&str>,
    ) -> Result<SearchProfileView, String> {
        let url = format!(
            "{}/v1/collections/{}/search-profiles/{}",
            self.base_url.trim_end_matches('/'),
            collection,
            profile
        );
        let mut builder = self.http.get(url);
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe search profile service unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<SearchProfileView>()
                .await
                .map_err(|_| "Ketebe search profile service unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe search profile request failed: {} {code}",
            status.as_u16()
        ))
    }

    async fn profile_request(
        &self,
        mut builder: reqwest::RequestBuilder,
        bearer_token: Option<&str>,
    ) -> Result<Vec<SearchProfileView>, String> {
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| "Ketebe search profile service unavailable".to_string())?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<Vec<SearchProfileView>>()
                .await
                .map_err(|_| "Ketebe search profile service unavailable".to_string());
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(format!(
            "Ketebe search profile request failed: {} {code}",
            status.as_u16()
        ))
    }

    pub async fn fetch_records(
        &self,
        collection: &str,
        ids: Vec<AgentRecordId>,
        fields: Vec<String>,
        bearer_token: Option<&str>,
    ) -> Result<GetRecordsOutput, RecordFetchError> {
        let url = format!(
            "{}/v0/collections/{}/records:fetch",
            self.base_url.trim_end_matches('/'),
            collection
        );
        let request = FetchRecordsRequest {
            ids: ids.into_iter().map(Into::into).collect(),
            fields,
        };
        let mut builder = self.http.post(url).json(&request);
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| RecordFetchError::Transport)?;
        let status = response.status();
        if status.is_success() {
            let response = response
                .json::<FetchRecordsResponse>()
                .await
                .map_err(|_| RecordFetchError::Transport)?;
            return Ok(GetRecordsOutput {
                records: response.records.into_iter().map(RecordView::from).collect(),
                missing: response.missing.into_iter().map(Into::into).collect(),
            });
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(RecordFetchError::Api {
            status: status.as_u16(),
            code,
        })
    }

    pub async fn search_many_params(
        &self,
        params: SearchManyParams,
        bearer_token: Option<&str>,
    ) -> Result<SearchManyOutput, String> {
        params.validate()?;
        let mut results = Vec::with_capacity(params.collections.len());
        let mut merge_input = Vec::new();

        for target in &params.collections {
            let collection = target.collection.clone();
            match self
                .search_params(params.search_params_for(target), bearer_token)
                .await
            {
                Ok(output) => {
                    for (index, hit) in output.hits.iter().cloned().enumerate() {
                        merge_input.push(ProvenancedSearchHit {
                            source_collection: collection.clone(),
                            source_rank: index + 1,
                            hit,
                        });
                    }
                    results.push(CollectionSearchResult {
                        collection,
                        status: CollectionSearchStatus::Ok,
                        mode: Some(output.mode),
                        hits: output.hits,
                        explain: output.explain,
                        error: None,
                    });
                }
                Err(error) => {
                    results.push(CollectionSearchResult {
                        collection,
                        status: CollectionSearchStatus::Error,
                        mode: None,
                        hits: Vec::new(),
                        explain: None,
                        error: Some(McpError::from_stable_message(&error)),
                    });
                }
            }
        }

        Ok(SearchManyOutput {
            results,
            merge_input,
        })
    }

    pub async fn search_fused_params(
        &self,
        params: FusedSearchParams,
        bearer_token: Option<&str>,
    ) -> Result<FusedSearchOutput, String> {
        params.validate()?;
        let mut results = Vec::with_capacity(params.collections.len());
        for target in &params.collections {
            let collection = target.collection.clone();
            match self
                .search_params_with_rerank(
                    params.search_params_for(target),
                    params.rerank.as_ref(),
                    bearer_token,
                )
                .await
            {
                Ok(output) => results.push(CollectionSearchResult {
                    collection,
                    status: CollectionSearchStatus::Ok,
                    mode: Some(output.mode),
                    hits: output.hits,
                    explain: output.explain,
                    error: None,
                }),
                Err(error) => results.push(CollectionSearchResult {
                    collection,
                    status: CollectionSearchStatus::Error,
                    mode: None,
                    hits: Vec::new(),
                    explain: None,
                    error: Some(McpError::from_stable_message(&error)),
                }),
            }
        }
        Ok(fuse_results(&params, results))
    }

    pub async fn search_params(
        &self,
        params: SearchParams,
        bearer_token: Option<&str>,
    ) -> Result<SearchOutput, String> {
        self.search_params_with_rerank(params, None, bearer_token)
            .await
    }

    async fn search_params_with_rerank(
        &self,
        params: SearchParams,
        rerank: Option<&ServerRerankParams>,
        bearer_token: Option<&str>,
    ) -> Result<SearchOutput, String> {
        let fields = params.fields.clone();
        let prefer_recent = params.prefer_recent;
        let (collection, mode, mut request) = params.into_request()?;
        if matches!(mode, SearchMode::Dense | SearchMode::Hybrid) && request.vector.is_none() {
            let text = request.text.as_deref().ok_or_else(|| {
                "Ketebe search request failed: embedding_text_required".to_string()
            })?;
            request.vector = Some(
                self.embed_query(&collection, text, bearer_token)
                    .await
                    .map_err(|error| error.stable_message())?,
            );
            if mode == SearchMode::Dense {
                request.text = None;
            }
        }
        let response = self
            .search_with_rerank(&collection, &request, rerank, bearer_token)
            .await
            .map_err(|error| error.stable_message())?;
        Ok(response.project(mode, &fields, prefer_recent))
    }

    async fn embed_query(
        &self,
        collection: &str,
        text: &str,
        bearer_token: Option<&str>,
    ) -> Result<Vec<f32>, SearchError> {
        let url = format!(
            "{}/v1/collections/{}/query:embed",
            self.base_url.trim_end_matches('/'),
            collection
        );
        let mut builder = self.http.post(url).json(&QueryEmbeddingRequest { text });
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder.send().await.map_err(|_| SearchError::Transport)?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<QueryEmbeddingResponse>()
                .await
                .map(|value| value.vector)
                .map_err(|_| SearchError::Transport);
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(SearchError::Api {
            status: status.as_u16(),
            code,
        })
    }

    async fn search_with_rerank(
        &self,
        collection: &str,
        request: &SearchRequest,
        rerank: Option<&ServerRerankParams>,
        bearer_token: Option<&str>,
    ) -> Result<SearchResponse, SearchError> {
        let url = format!(
            "{}/v1/collections/{}/query",
            self.base_url.trim_end_matches('/'),
            collection
        );
        let mut body = serde_json::to_value(request).map_err(|_| SearchError::Transport)?;
        if let Some(rerank) = rerank {
            let Value::Object(object) = &mut body else {
                return Err(SearchError::Transport);
            };
            object.insert(
                "rerank".to_string(),
                serde_json::to_value(rerank).map_err(|_| SearchError::Transport)?,
            );
        }
        let mut builder = self.http.post(url).json(&body);
        if let Some(token) = bearer_token {
            builder = builder.bearer_auth(token);
        }
        let response = builder.send().await.map_err(|_| SearchError::Transport)?;
        let status = response.status();
        if status.is_success() {
            return response
                .json::<SearchResponse>()
                .await
                .map_err(|_| SearchError::Transport);
        }
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "http_error".to_string());
        Err(SearchError::Api {
            status: status.as_u16(),
            code,
        })
    }

    pub async fn authenticate(&self, bearer_token: &str) -> Result<(), AuthenticationProbeError> {
        let client = self
            .client_for(Some(bearer_token))
            .map_err(|_| AuthenticationProbeError::Unavailable)?;
        match client.list_collections().await {
            Ok(_) => Ok(()),
            Err(ketebe_sdk::Error::Api { status, .. }) if status.as_u16() == 401 => {
                Err(AuthenticationProbeError::Unauthenticated)
            }
            Err(ketebe_sdk::Error::Api { status, .. }) if status.as_u16() == 403 => {
                Err(AuthenticationProbeError::Forbidden)
            }
            Err(_) => Err(AuthenticationProbeError::Unavailable),
        }
    }
}

use crate::ketebe::KetebeApi;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct StreamCollectionParams {
    pub collection: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct StreamIngestionParams {
    pub collection: String,
    pub stream_id: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct CreateStreamIngestionParams {
    pub collection: String,
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    #[serde(default)]
    pub batch_max_records: Option<usize>,
    #[serde(default)]
    pub batch_linger_ms: Option<u64>,
    #[serde(default)]
    pub dlq_topic: Option<String>,
    #[serde(default)]
    pub security_protocol: Option<String>,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username_ref: Option<String>,
    #[serde(default)]
    pub sasl_password_ref: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct StreamIngestionView {
    pub id: String,
    pub collection: String,
    pub topic: String,
    pub group_id: String,
    pub state: String,
    pub consumer_lag_records: Option<u64>,
    pub failure_code: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ListStreamIngestionsOutput {
    pub streams: Vec<StreamIngestionView>,
}

#[derive(Serialize)]
struct CreateStreamRequest<'a> {
    brokers: &'a str,
    topic: &'a str,
    group_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_max_records: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_linger_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dlq_topic: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    security_protocol: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sasl_mechanism: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sasl_username_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sasl_password_ref: Option<&'a str>,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: String,
}

impl KetebeApi {
    pub async fn list_stream_ingestions(
        &self,
        collection: &str,
        bearer_token: Option<&str>,
    ) -> Result<ListStreamIngestionsOutput, String> {
        validate_collection(collection)?;
        let url = format!(
            "{}/v0/collections/{collection}/stream-ingestions",
            self.base_url
        );
        let response = self
            .send_stream_request(self.http.get(url), bearer_token)
            .await?;
        let streams = response
            .json::<Vec<StreamIngestionView>>()
            .await
            .map_err(|_| "Ketebe stream ingestion response invalid".to_string())?;
        Ok(ListStreamIngestionsOutput { streams })
    }

    pub async fn create_stream_ingestion(
        &self,
        params: CreateStreamIngestionParams,
        bearer_token: Option<&str>,
    ) -> Result<StreamIngestionView, String> {
        validate_collection(&params.collection)?;
        if params.brokers.trim().is_empty()
            || params.topic.trim().is_empty()
            || params.group_id.trim().is_empty()
        {
            return Err(
                "Ketebe stream ingestion request invalid: brokers, topic and group_id are required"
                    .to_string(),
            );
        }
        let url = format!(
            "{}/v0/collections/{}/stream-ingestions",
            self.base_url, params.collection
        );
        let body = CreateStreamRequest {
            brokers: &params.brokers,
            topic: &params.topic,
            group_id: &params.group_id,
            batch_max_records: params.batch_max_records,
            batch_linger_ms: params.batch_linger_ms,
            dlq_topic: params.dlq_topic.as_deref(),
            security_protocol: params.security_protocol.as_deref(),
            sasl_mechanism: params.sasl_mechanism.as_deref(),
            sasl_username_ref: params.sasl_username_ref.as_deref(),
            sasl_password_ref: params.sasl_password_ref.as_deref(),
        };
        let response = self
            .send_stream_request(self.http.post(url).json(&body), bearer_token)
            .await?;
        response
            .json::<StreamIngestionView>()
            .await
            .map_err(|_| "Ketebe stream ingestion response invalid".to_string())
    }

    pub async fn get_stream_ingestion(
        &self,
        params: StreamIngestionParams,
        bearer_token: Option<&str>,
    ) -> Result<StreamIngestionView, String> {
        self.stream_action(params, None, bearer_token).await
    }

    pub async fn pause_stream_ingestion(
        &self,
        params: StreamIngestionParams,
        bearer_token: Option<&str>,
    ) -> Result<StreamIngestionView, String> {
        self.stream_action(params, Some("pause"), bearer_token)
            .await
    }

    pub async fn resume_stream_ingestion(
        &self,
        params: StreamIngestionParams,
        bearer_token: Option<&str>,
    ) -> Result<StreamIngestionView, String> {
        self.stream_action(params, Some("resume"), bearer_token)
            .await
    }

    async fn stream_action(
        &self,
        params: StreamIngestionParams,
        action: Option<&str>,
        bearer_token: Option<&str>,
    ) -> Result<StreamIngestionView, String> {
        validate_collection(&params.collection)?;
        if params.stream_id.trim().is_empty() {
            return Err(
                "Ketebe stream ingestion request invalid: stream_id is required".to_string(),
            );
        }
        let mut url = format!(
            "{}/v0/collections/{}/stream-ingestions/{}",
            self.base_url, params.collection, params.stream_id
        );
        if let Some(action) = action {
            url.push('/');
            url.push_str(action);
        }
        let request = if action.is_some() {
            self.http.post(url)
        } else {
            self.http.get(url)
        };
        let response = self.send_stream_request(request, bearer_token).await?;
        response
            .json::<StreamIngestionView>()
            .await
            .map_err(|_| "Ketebe stream ingestion response invalid".to_string())
    }

    async fn send_stream_request(
        &self,
        request: reqwest::RequestBuilder,
        bearer_token: Option<&str>,
    ) -> Result<reqwest::Response, String> {
        let request = match bearer_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        let response = request
            .send()
            .await
            .map_err(|_| "Ketebe stream ingestion service unavailable".to_string())?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let code = response
            .json::<ErrorEnvelope>()
            .await
            .map(|value| value.error.code)
            .unwrap_or_else(|_| "stream_ingestion_error".to_string());
        Err(format!(
            "Ketebe stream ingestion request failed: {status} {code}"
        ))
    }
}

fn validate_collection(collection: &str) -> Result<(), String> {
    if collection.trim().is_empty() {
        Err("Ketebe stream ingestion request invalid: collection is required".to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_view_has_no_broker_or_secret_fields() {
        let view = StreamIngestionView {
            id: "stream-docs".to_string(),
            collection: "docs".to_string(),
            topic: "documents".to_string(),
            group_id: "ketebe-docs".to_string(),
            state: "running".to_string(),
            consumer_lag_records: Some(1),
            failure_code: None,
        };
        let value = serde_json::to_value(view).unwrap();
        let serde_json::Value::Object(fields) = value else {
            panic!("expected object");
        };
        assert!(!fields.contains_key("brokers"));
        assert!(!fields.contains_key("sasl_username_ref"));
        assert!(!fields.contains_key("sasl_password_ref"));
        assert!(!fields.contains_key("credentials"));
    }
}

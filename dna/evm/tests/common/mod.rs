use serde::Deserialize;
use serde_json::{json, Value};
use wiremock::{http::Method, Match, Request, Respond, ResponseTemplate};

#[derive(Debug, Deserialize)]
struct RpcRequest {
    id: u64,
    method: String,
    params: Value,
}

pub struct RpcRequestMatcher {
    method: String,
    params: Value,
}

pub struct RpcResponse {
    result: Value,
}

pub struct RpcErrorResponse {
    message: String,
}

pub fn rpc_request<P>(method: impl Into<String>, params: P) -> RpcRequestMatcher
where
    P: serde::Serialize,
{
    RpcRequestMatcher {
        method: method.into(),
        params: serde_json::to_value(params).expect("failed to serialize JSON params"),
    }
}

impl RpcResponse {
    pub fn new(result: Value) -> Self {
        Self { result }
    }
}

impl RpcErrorResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Match for RpcRequestMatcher {
    fn matches(&self, req: &Request) -> bool {
        if req.method != Method::Post {
            return false;
        }

        if let Ok(rpc_req) = serde_json::from_slice::<RpcRequest>(&req.body) {
            rpc_req.method == self.method && rpc_req.params == self.params
        } else {
            false
        }
    }
}

impl Respond for RpcResponse {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        if let Ok(rpc_req) = serde_json::from_slice::<RpcRequest>(&req.body) {
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": rpc_req.id,
                "result": self.result,
            }))
        } else {
            ResponseTemplate::new(400)
        }
    }
}

impl Respond for RpcErrorResponse {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        if let Ok(rpc_req) = serde_json::from_slice::<RpcRequest>(&req.body) {
            ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0",
                "id": rpc_req.id,
                "error": {
                    "code": -32602,
                    "message": self.message,
                }
            }))
        } else {
            ResponseTemplate::new(400)
        }
    }
}

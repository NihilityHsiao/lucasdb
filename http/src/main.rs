use std::{collections::HashMap, path::PathBuf, sync::Arc};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use lucasdb::{db::Engine, options::EngineOptions};

async fn ping() -> &'static str {
    return "ping";
}

struct A {}
impl IntoResponse for A {
    fn into_response(self) -> axum::response::Response {
        todo!()
    }
}

// get: /put
async fn handler_put(
    State(engine): State<Arc<Engine>>,
    Json(data): Json<HashMap<String, String>>,
) -> impl IntoResponse {
    println!("received: {:?}", data);
    for (key, value) in data.iter() {
        if let Err(_) = engine.put(Bytes::from(key.to_string()), Bytes::from(value.to_string())) {
            let resp = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("failed to put value in engine"))
                .unwrap();
            return resp;
        }
    }
    let resp = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("OK"))
        .unwrap();
    return resp;
}

async fn handler_get(
    State(engine): State<Arc<Engine>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let key = Bytes::from(key);
    let value_res = engine.get(key);
    let value = match value_res {
        Ok(value) => value,
        Err(e) => match e {
            lucasdb::errors::Errors::KeyNotFound => {
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("key not found"))
                    .unwrap();
                return resp;
            }
            _ => {
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("failed to get value in engine"))
                    .unwrap();
                return resp;
            }
        },
    };

    let resp = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(value))
        .unwrap();
    resp
}

async fn handler_delete(
    State(engine): State<Arc<Engine>>,
    Path(key): Path<String>,
) -> impl IntoResponse {
    let key = Bytes::from(key);
    let value_res = engine.delete(key);
    match value_res {
        Ok(value) => value,
        Err(e) => match e {
            lucasdb::errors::Errors::KeyNotFound => {
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("key not found"))
                    .unwrap();
                return resp;
            }
            _ => {
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from("failed to delete value in engine"))
                    .unwrap();
                return resp;
            }
        },
    };

    let resp = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("OK"))
        .unwrap();
    resp
}

async fn handler_listkeys(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    let keys = match engine.list_keys() {
        Ok(keys) => keys,
        Err(_) => todo!(),
    };

    let keys = keys
        .into_iter()
        .map(|key| String::from_utf8(key.to_vec()).unwrap())
        .collect::<Vec<String>>();

    Json(keys)
}

async fn handler_stat(State(engine): State<Arc<Engine>>) -> impl IntoResponse {
    let stat = match engine.stat() {
        Ok(stat) => stat,
        Err(_) => {
            todo!()
        }
    };

    let mut status_map = HashMap::new();
    status_map.insert("key_num", stat.key_num);
    status_map.insert("data_file_num", stat.data_file_num);
    status_map.insert("reclaim_size", stat.reclaim_size);
    status_map.insert("disk_size", stat.disk_size);
    Json(status_map)
}

fn init_router(engine: Arc<Engine>) -> Router {
    let api = Router::new()
        .route("/ping", get(ping))
        .route("/put", post(handler_put).with_state(engine.clone()))
        .route("/get/:key", get(handler_get).with_state(engine.clone()))
        .route(
            "/listkeys",
            get(handler_listkeys).with_state(engine.clone()),
        )
        .route(
            "/delete/:key",
            delete(handler_delete).with_state(engine.clone()),
        )
        .route("/stat", get(handler_stat).with_state(engine.clone()));
    let router = Router::new().nest("/lucasdb", api);
    router
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // 启动 engine 实例
    let mut opts = EngineOptions::default();
    opts.dir_path = PathBuf::from("../tmp/lucasdb-http");
    let engine = Arc::new(Engine::open(opts).unwrap());

    // 启动http服务
    let router = init_router(engine.clone());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:53309").await?;
    axum::serve(listener, router).await?;
    Ok(())
}

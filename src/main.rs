use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, patch, post},
    Json, Router,
};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};

use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use chrono::Utc;
use sha2::{Digest, Sha256};
use time::Duration;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fs,
    net::SocketAddr,
    path::{Path as StdPath, PathBuf},
    str::FromStr,
};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, task};
use uuid::Uuid;
use sevenz_rust::{lzma, SevenZWriter};








const MAX_TEXT_VIEW_BYTES: usize = 50 * 1024 * 1024; // Maximum file size for online text viewing.
const MAX_TEXT_EDIT_BYTES: usize = 20 * 1024 * 1024; // Maximum file size for online text editing.

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!(
    "If you'd like to sponsor me, you can use\nBitcoin: {}\nEthereum: {}\nBSC: {}",
    "bc1qxqfhumpqtnxrznkx9r4xsp8m6zsedtgusjns7p",
    "0x2d92f9e4d8ac7effa9cd7cd5eccd364cac7c201b",
    "0x2d92f9e4d8ac7effa9cd7cd5eccd364cac7c201b"
    );


    let data_directory = PathBuf::from("./drive_data");
    let object_storage_directory = data_directory.join("objects");
    let temporary_directory = data_directory.join("tmp");

    tokio::fs::create_dir_all(&object_storage_directory).await?;
    tokio::fs::create_dir_all(&temporary_directory).await?;

    let database_path = data_directory.join("drive.sqlite3");
    let database_url = format!("sqlite://{}", database_path.display());

    let database_options = SqliteConnectOptions::from_str(&database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true);

    let database_pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(database_options)
        .await?;

    init_database(&database_pool).await?;

    let state = AppState {
        database: database_pool,
        object_storage_directory,
        temporary_directory,
    };

    let application = Router::new()
        .route("/", get(index))
        .route("/login", get(index))
        .route("/register", get(index))
        .route("/view/{node_id}", get(view_page))
        .route("/shares", get(shares_manage_page))
        .route("/s/{token}", get(share_page))
        .route("/api/health", get(health_check))
        .route("/api/me", get(me))
        .route("/api/auth/register", post(register))
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/root", get(get_root))
        .route("/api/nodes/{node_id}", get(get_node).delete(delete_node))
        .route("/api/nodes/{folder_id}/children", get(list_children))
        .route("/api/nodes/{node_id}/rename", patch(rename_node))
        .route("/api/nodes/{node_id}/move", patch(move_node))
        .route("/api/nodes/{node_id}/breadcrumbs", get(get_breadcrumbs))
        .route("/api/nodes/{node_id}/download", get(download_node))
        .route("/api/nodes/download-selected", post(download_selected_nodes))
        .route("/api/nodes/delete-selected", post(delete_selected_nodes))
        .route("/api/nodes/move-selected", post(move_selected_nodes))
        .route("/api/nodes/{node_id}/preview", get(preview_node))
        .route("/api/nodes/{node_id}/text", get(read_text_node).put(update_text_node))
        .route(
            "/api/nodes/{node_id}/share",
            get(get_node_share).post(create_share).delete(cancel_node_share),
        )
        .route("/api/folders", post(create_folder))
        .route("/api/files", post(upload_file))
        .route("/api/shares", get(list_shares))
        .route("/api/shares/{token}", delete(cancel_share_by_token))
        .route("/api/public/shares/{token}", get(public_share_info))
        .route("/api/public/shares/{token}/download", get(public_share_download))
        .route("/api/public/shares/{token}/preview", get(public_share_preview))
        .route("/api/public/shares/{token}/text", get(public_share_text))
        .route("/api/public/shares/{token}/nodes/{node_id}/children", get(public_share_children))
        .route("/api/public/shares/{token}/nodes/{node_id}/breadcrumbs", get(public_share_breadcrumbs))
        .route("/api/public/shares/{token}/nodes/{node_id}/download", get(public_share_node_download))
        .route("/api/public/shares/{token}/download-selected", post(public_share_selected_download))
        .route("/api/public/shares/{token}/nodes/{node_id}/preview", get(public_share_node_preview))
        .route("/api/public/shares/{token}/nodes/{node_id}/text", get(public_share_node_text))
        .layer(DefaultBodyLimit::disable())
        .with_state(state.clone());

    let server_address = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("RustDrive is running at: http://{}", server_address);
    println!("SQLite database: {}", database_path.display());
    println!("Object directory: {}", state.object_storage_directory.display());
    println!("Temporary directory: {}", state.temporary_directory.display());

    let listener = tokio::net::TcpListener::bind(server_address).await?;
    axum::serve(listener, application).await?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    database: SqlitePool, // SQLite connection pool.
    object_storage_directory: PathBuf, // File object storage directory.
    temporary_directory: PathBuf, // Temporary directory for 7z archives.
}

// Initialize SQLite tables and indexes.
async fn init_database(database: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            root_node_id TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        "#,
    )
    .execute(database)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        "#,
    )
    .execute(database)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            parent_id TEXT,
            name TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('file', 'folder')),
            size INTEGER NOT NULL DEFAULT 0,
            mime_type TEXT,
            storage_key TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(parent_id) REFERENCES nodes(id) ON DELETE CASCADE
        );
        "#,
    )
    .execute(database)
    .await?;

    sqlx::query(
        r#"
        CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_unique_name
        ON nodes(user_id, parent_id, lower(name));
        "#,
    )
    .execute(database)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_nodes_parent
        ON nodes(user_id, parent_id);
        "#,
    )
    .execute(database)
    .await?;

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS shares (
            token TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(owner_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE,
            UNIQUE(owner_id, node_id)
        );
        "#,
    )
    .execute(database)
    .await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize)]
struct UserDto {
    id: String, // User ID.
    username: String, // Login username.
    root_node_id: String, // User root folder node ID.
}

#[derive(Debug, Deserialize)]
struct AuthRequest {
    username: String, // Username.
    password: String, // Login or registration password.
}

#[derive(Debug, Serialize)]
struct AuthResponse {
    user: UserDto,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum NodeKind {
    File, // File node.
    Folder, // Folder node.
}

impl NodeKind {
    fn from_db_str(value: &str) -> ApiResult<Self> {
        match value {
            "file" => Ok(NodeKind::File),
            "folder" => Ok(NodeKind::Folder),
            _ => Err(ApiError::Internal("Invalid node type in database".to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct NodeDto {
    id: String, // Node ID.
    parent_id: Option<String>, // Parent folder ID.
    name: String, // File or folder name.
    kind: NodeKind, // Node type.
    size: i64, // File size; folders are 0.
    mime_type: Option<String>, // MIME type.
    created_at: String, // Created time.
    updated_at: String, // Updated time.
    shared: bool, // Whether this node is shared.
    share_token: Option<String>, // Share token.
    preview_kind: String, // Preview type.
    editable_text: bool, // Whether text editing is supported.
}

#[derive(Debug, Clone)]
struct NodeRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    kind: NodeKind,
    size: i64,
    mime_type: Option<String>,
    storage_key: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct CreateFolderRequest {
    parent_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct RenameNodeRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct MoveNodeRequest {
    new_parent_id: String,
}

#[derive(Debug, Deserialize)]
struct SelectedDownloadRequest {
    node_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SelectedMoveRequest {
    node_ids: Vec<String>,
    new_parent_id: String,
}

#[derive(Debug, Deserialize)]
struct UpdateTextRequest {
    content: String,
}

#[derive(Debug, Serialize)]
struct TextContentResponse {
    id: String,
    name: String,
    content: String,
    readonly: bool,
    encoding: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct BreadcrumbItem {
    id: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct BreadcrumbResponse {
    items: Vec<BreadcrumbItem>,
}

#[derive(Debug, Serialize)]
struct ShareDto {
    token: String, // Share token.
    url: String, // Share access path.
    node: NodeDto, // Shared node.
    created_at: String, // Share creation time.
}

#[derive(Debug)]
enum ApiError {
    Unauthorized(String),
    BadRequest(String),
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::Unauthorized(message) => {
                (StatusCode::UNAUTHORIZED, "unauthorized", message)
            }
            ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            ApiError::Conflict(message) => (StatusCode::CONFLICT, "conflict", message),
            ApiError::Internal(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", message)
            }
        };

        eprintln!("[api error] status={} code={} message={}", status.as_u16(), code, message);

        (
            status,
            Json(json!({
                "code": code,
                "message": message,
            })),
        )
            .into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn share_page() -> Html<&'static str> {
    Html(SHARE_HTML)
}

async fn view_page() -> Html<&'static str> {
    Html(VIEW_HTML)
}

async fn shares_manage_page() -> Html<&'static str> {
    Html(SHARES_MANAGE_HTML)
}

async fn health_check() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

// Register a user and create the user's root folder.
async fn register(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<AuthRequest>,
) -> ApiResult<impl IntoResponse> {
    validate_username(&request.username)?;
    validate_password(&request.password)?;

    let user_id = Uuid::new_v4().to_string();
    let root_node_id = Uuid::new_v4().to_string();
    let now = now_string();
    let password_hash = hash_password(&request.password)?;

    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to start transaction: {}", error)))?;

    let user_insert = sqlx::query(
        r#"
        INSERT INTO users(id, username, password_hash, root_node_id, created_at)
        VALUES (?, ?, ?, ?, ?)
        "#,
    )
    .bind(&user_id)
    .bind(request.username.trim())
    .bind(&password_hash)
    .bind(&root_node_id)
    .bind(&now)
    .execute(&mut *transaction)
    .await;

    if let Err(error) = user_insert {
        let msg = error.to_string().to_lowercase();
        if msg.contains("unique") {
            return Err(ApiError::Conflict("Username already exists".to_string()));
        }

        return Err(ApiError::Internal(format!("Failed to create user: {}", error)));
    }

    sqlx::query(
        r#"
        INSERT INTO nodes(
            id, user_id, parent_id, name, kind, size,
            mime_type, storage_key, created_at, updated_at
        )
        VALUES (?, ?, NULL, ?, 'folder', 0, NULL, NULL, ?, ?)
        "#,
    )
    .bind(&root_node_id)
    .bind(&user_id)
    .bind("My Drive")
    .bind(&now)
    .bind(&now)
    .execute(&mut *transaction)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to create root folder: {}", error)))?;

    transaction.commit()
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to commit transaction: {}", error)))?;

    let (jar, user) = create_session_and_cookie(&state, jar, &user_id).await?;

    Ok((jar, Json(AuthResponse { user })))
}

// Log in the user and write the session cookie.
async fn login(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<AuthRequest>,
) -> ApiResult<impl IntoResponse> {
    let database_row = sqlx::query(
        r#"
        SELECT id, username, password_hash, root_node_id
        FROM users
        WHERE username = ?
        "#,
    )
    .bind(request.username.trim())
    .fetch_optional(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query user: {}", error)))?
    .ok_or_else(|| ApiError::Unauthorized("Incorrect username or password".to_string()))?;

    let password_hash: String = database_row.get("password_hash");
    verify_password(&request.password, &password_hash)?;

    let user_id: String = database_row.get("id");
    let (jar, user) = create_session_and_cookie(&state, jar, &user_id).await?;

    Ok((jar, Json(AuthResponse { user })))
}

async fn logout(State(state): State<AppState>, jar: CookieJar) -> ApiResult<impl IntoResponse> {
    if let Some(cookie) = jar.get("sid") {
        let token = cookie.value().to_string();

        let _ = sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(token)
            .execute(&state.database)
            .await;
    }

    let remove_cookie = Cookie::build(("sid", ""))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(Duration::seconds(0))
        .build();

    Ok((jar.add(remove_cookie), Json(json!({ "ok": true }))))
}

async fn me(State(state): State<AppState>, jar: CookieJar) -> ApiResult<Json<AuthResponse>> {
    let user = require_user(&state, &jar).await?;
    Ok(Json(AuthResponse { user }))
}

async fn get_root(State(state): State<AppState>, jar: CookieJar) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;
    let node = get_node_dto_by_id(&state.database, &user.id, &user.root_node_id).await?;
    Ok(Json(node))
}

async fn get_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;
    validate_uuid_text(&node_id, "node_id")?;

    let node = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;
    Ok(Json(node))
}

async fn list_children(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(folder_id): Path<String>,
) -> ApiResult<Json<ListResponse<NodeDto>>> {
    let user = require_user(&state, &jar).await?;
    validate_uuid_text(&folder_id, "folder_id")?;

    let folder = get_node_record_by_id(&state.database, &user.id, &folder_id).await?;
    if folder.kind != NodeKind::Folder {
        return Err(ApiError::BadRequest("Target is not a folder".to_string()));
    }

    let database_rows = sqlx::query(
        r#"
        SELECT n.*, s.token AS share_token
        FROM nodes n
        LEFT JOIN shares s ON s.node_id = n.id AND s.owner_id = n.user_id
        WHERE n.user_id = ? AND n.parent_id = ?
        ORDER BY CASE n.kind WHEN 'folder' THEN 0 ELSE 1 END, lower(n.name) ASC
        "#,
    )
    .bind(&user.id)
    .bind(&folder_id)
    .fetch_all(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to list directory: {}", error)))?;

    let mut items = Vec::new();

    for database_row in database_rows {
        items.push(row_to_node_dto(&database_row)?);
    }

    Ok(Json(ListResponse { items }))
}

async fn create_folder(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<CreateFolderRequest>,
) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&request.parent_id, "parent_id")?;
    validate_node_name(&request.name)?;

    let parent = get_node_record_by_id(&state.database, &user.id, &request.parent_id).await?;
    if parent.kind != NodeKind::Folder {
        return Err(ApiError::BadRequest("Parent node is not a folder".to_string()));
    }

    ensure_no_name_conflict(&state.database, &user.id, &request.parent_id, &request.name, None).await?;

    let id = Uuid::new_v4().to_string();
    let now = now_string();

    sqlx::query(
        r#"
        INSERT INTO nodes(
            id, user_id, parent_id, name, kind, size,
            mime_type, storage_key, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, 'folder', 0, NULL, NULL, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(&user.id)
    .bind(&request.parent_id)
    .bind(request.name.trim())
    .bind(&now)
    .bind(&now)
    .execute(&state.database)
    .await
    .map_err(|error| db_conflict_or_internal(error, "Failed to create folder"))?;

    let node = get_node_dto_by_id(&state.database, &user.id, &id).await?;
    Ok(Json(node))
}

// Upload files, including multi-file uploads and folder paths.
async fn upload_file(
    State(state): State<AppState>,
    jar: CookieJar,
    mut multipart: Multipart,
) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;

    let mut parent_id: Option<String> = None;
    let mut file_name: Option<String> = None;
    let mut mime_type: Option<String> = None;
    let mut storage_key: Option<String> = None;
    let mut written_size: i64 = 0;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::BadRequest(format!("Multipart data error: {}", error)))?
    {
        let field_name = field.name().unwrap_or("").to_string();

        match field_name.as_str() {
            "parent_id" => {
                let value = field
                    .text()
                    .await
                    .map_err(|error| ApiError::BadRequest(format!("parent_id error: {}", error)))?;

                validate_uuid_text(&value, "parent_id")?;
                parent_id = Some(value);
            }
            "name" => {
                let value = field
                    .text()
                    .await
                    .map_err(|error| ApiError::BadRequest(format!("name error: {}", error)))?;

                if !value.trim().is_empty() {
                    file_name = Some(value);
                }
            }
            "file" => {
                let uploaded_name = field.file_name().map(|s| s.to_string());
                if uploaded_name.is_some() && file_name.is_none() {
                    file_name = uploaded_name;
                }

                mime_type = field.content_type().map(|s| s.to_string());

                let key = Uuid::new_v4().to_string();
                let object_path = state.object_storage_directory.join(&key);
                let mut output = tokio::fs::File::create(&object_path)
                    .await
                    .map_err(|error| ApiError::Internal(format!("Failed to create file: {}", error)))?;

                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|error| ApiError::BadRequest(format!("Failed to read file chunk: {}", error)))?
                {
                    written_size += chunk.len() as i64;
                    output
                        .write_all(&chunk)
                        .await
                        .map_err(|error| ApiError::Internal(format!("Failed to write file: {}", error)))?;
                }

                output
                    .flush()
                    .await
                    .map_err(|error| ApiError::Internal(format!("Failed to flush file: {}", error)))?;

                storage_key = Some(key);
            }
            _ => {}
        }
    }

    let parent_id =
        parent_id.ok_or_else(|| ApiError::BadRequest("Missing parent_id".to_string()))?;

    let file_name = file_name.ok_or_else(|| ApiError::BadRequest("Missing file name".to_string()))?;

    let storage_key = storage_key.ok_or_else(|| ApiError::BadRequest("Missing file content".to_string()))?;
    let object_path = state.object_storage_directory.join(&storage_key);

    if let Err(error) = validate_node_name(&file_name) {
        let _ = tokio::fs::remove_file(&object_path).await;
        return Err(error);
    }

    let parent = match get_node_record_by_id(&state.database, &user.id, &parent_id).await {
        Ok(parent) => parent,
        Err(error) => {
            let _ = tokio::fs::remove_file(&object_path).await;
            return Err(error);
        }
    };

    if parent.kind != NodeKind::Folder {
        let _ = tokio::fs::remove_file(&object_path).await;
        return Err(ApiError::BadRequest("Parent node is not a folder".to_string()));
    }

    let final_file_name = match resolve_upload_file_name(
        &state,
        &user.id,
        &parent_id,
        &file_name,
        &object_path,
        written_size,
    )
    .await
    {
        Ok(UploadNameResolution::UseName(name)) => name,
        Ok(UploadNameResolution::AlreadyExists(existing_node_id)) => {
            let _ = tokio::fs::remove_file(&object_path).await;
            let node = get_node_dto_by_id(&state.database, &user.id, &existing_node_id).await?;
            return Ok(Json(node));
        }
        Err(error) => {
            let _ = tokio::fs::remove_file(&object_path).await;
            return Err(error);
        }
    };

    let id = Uuid::new_v4().to_string();
    let now = now_string();
    let final_mime = mime_type.unwrap_or_else(|| guess_mime_by_name(&final_file_name).to_string());

    let insert_result = sqlx::query(
        r#"
        INSERT INTO nodes(
            id, user_id, parent_id, name, kind, size,
            mime_type, storage_key, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, 'file', ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(&user.id)
    .bind(&parent_id)
    .bind(final_file_name.trim())
    .bind(written_size)
    .bind(final_mime)
    .bind(&storage_key)
    .bind(&now)
    .bind(&now)
    .execute(&state.database)
    .await;

    if let Err(error) = insert_result {
        let _ = tokio::fs::remove_file(&object_path).await;
        return Err(db_conflict_or_internal(error, "Failed to upload file"));
    }

    let node = get_node_dto_by_id(&state.database, &user.id, &id).await?;
    Ok(Json(node))
}

async fn rename_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
    Json(request): Json<RenameNodeRequest>,
) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;
    validate_node_name(&request.name)?;

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;
    if node.parent_id.is_none() {
        return Err(ApiError::BadRequest("Cannot rename the root folder".to_string()));
    }

    let parent_id = node.parent_id.clone().unwrap();

    ensure_no_name_conflict(
        &state.database,
        &user.id,
        &parent_id,
        &request.name,
        Some(&node_id),
    )
    .await?;

    if node.kind == NodeKind::File {
        sqlx::query(
            r#"
            UPDATE nodes
            SET name = ?, mime_type = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
            "#,
        )
        .bind(request.name.trim())
        .bind(guess_mime_by_name(&request.name))
        .bind(now_string())
        .bind(&node_id)
        .bind(&user.id)
        .execute(&state.database)
        .await
        .map_err(|error| db_conflict_or_internal(error, "Rename failed"))?;
    } else {
        sqlx::query(
            r#"
            UPDATE nodes
            SET name = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
            "#,
        )
        .bind(request.name.trim())
        .bind(now_string())
        .bind(&node_id)
        .bind(&user.id)
        .execute(&state.database)
        .await
        .map_err(|error| db_conflict_or_internal(error, "Rename failed"))?;
    }

    let node = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;
    Ok(Json(node))
}

async fn move_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
    Json(request): Json<MoveNodeRequest>,
) -> ApiResult<Json<NodeDto>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;
    validate_uuid_text(&request.new_parent_id, "new_parent_id")?;

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;
    if node.parent_id.is_none() {
        return Err(ApiError::BadRequest("Cannot move the root folder".to_string()));
    }

    let target = get_node_record_by_id(&state.database, &user.id, &request.new_parent_id).await?;
    if target.kind != NodeKind::Folder {
        return Err(ApiError::BadRequest("Target is not a folder".to_string()));
    }

    if node.parent_id.as_deref() == Some(request.new_parent_id.as_str()) {
        let dto = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;
        return Ok(Json(dto));
    }

    if node.kind == NodeKind::Folder {
        ensure_not_move_folder_into_descendant(
            &state.database,
            &user.id,
            &node_id,
            &request.new_parent_id,
        )
        .await?;
    }

    let moved_node = move_node_record_to_parent(&state, &user.id, &node, &request.new_parent_id).await?;
    Ok(Json(moved_node))
}

async fn move_selected_nodes(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<SelectedMoveRequest>,
) -> ApiResult<Json<ListResponse<NodeDto>>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&request.new_parent_id, "new_parent_id")?;

    if request.node_ids.is_empty() {
        return Err(ApiError::BadRequest("Please select files or folders to move".to_string()));
    }

    let target = get_node_record_by_id(&state.database, &user.id, &request.new_parent_id).await?;
    if target.kind != NodeKind::Folder {
        return Err(ApiError::BadRequest("Target is not a folder".to_string()));
    }

    let mut moved_items = Vec::new();
    let mut seen_node_ids = HashSet::new();

    for node_id in &request.node_ids {
        validate_uuid_text(node_id, "node_id")?;
        if !seen_node_ids.insert(node_id.clone()) {
            continue;
        }

        let node = get_node_record_by_id(&state.database, &user.id, node_id).await?;
        if node.parent_id.is_none() {
            return Err(ApiError::BadRequest("Cannot move the root folder".to_string()));
        }

        if node.parent_id.as_deref() == Some(request.new_parent_id.as_str()) {
            moved_items.push(get_node_dto_by_id(&state.database, &user.id, node_id).await?);
            continue;
        }

        if node.kind == NodeKind::Folder {
            ensure_not_move_folder_into_descendant(
                &state.database,
                &user.id,
                node_id,
                &request.new_parent_id,
            )
            .await?;
        }

        moved_items.push(move_node_record_to_parent(&state, &user.id, &node, &request.new_parent_id).await?);
    }

    Ok(Json(ListResponse { items: moved_items }))
}

async fn delete_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<StatusCode> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;
    delete_nodes_for_owner(&state, &user.id, &[node_id]).await?;

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_selected_nodes(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<SelectedDownloadRequest>,
) -> ApiResult<StatusCode> {
    let user = require_user(&state, &jar).await?;

    if request.node_ids.is_empty() {
        return Err(ApiError::BadRequest("Please select files or folders to delete".to_string()));
    }

    for node_id in &request.node_ids {
        validate_uuid_text(node_id, "node_id")?;
    }

    delete_nodes_for_owner(&state, &user.id, &request.node_ids).await?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_breadcrumbs(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Json<BreadcrumbResponse>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let items = build_breadcrumbs(&state.database, &user.id, &node_id).await?;

    Ok(Json(BreadcrumbResponse { items }))
}

async fn download_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Response> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    download_node_for_owner(&state, &user.id, &node_id).await
}

async fn download_selected_nodes(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(request): Json<SelectedDownloadRequest>,
) -> ApiResult<Response> {
    let user = require_user(&state, &jar).await?;

    if request.node_ids.is_empty() {
        return Err(ApiError::BadRequest("Please select files or folders to download".to_string()));
    }

    for node_id in &request.node_ids {
        validate_uuid_text(node_id, "node_id")?;
        let node = get_node_record_by_id(&state.database, &user.id, node_id).await?;
        if node.parent_id.is_none() {
            return Err(ApiError::BadRequest("Cannot download the root folder".to_string()));
        }
    }

    download_selected_nodes_for_owner(
        &state,
        &user.id,
        &request.node_ids,
        "selected.7z",
    )
    .await
}

async fn preview_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;
    preview_file_record(&state, &node, &headers).await
}

async fn read_text_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Json<TextContentResponse>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;
    let (content, encoding) = read_text_content(&state, &node).await?;

    Ok(Json(TextContentResponse {
        id: node.id,
        name: node.name,
        content,
        readonly: false,
        encoding,
        updated_at: node.updated_at,
    }))
}

// Save online-edited text and write it back as UTF-8.
async fn update_text_node(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
    Json(request): Json<UpdateTextRequest>,
) -> ApiResult<Json<TextContentResponse>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    if request.content.len() > MAX_TEXT_EDIT_BYTES {
        return Err(ApiError::BadRequest("The text file is too large for online editing".to_string()));
    }

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;

    if node.kind != NodeKind::File || !is_editable_text_name(&node.name) {
        return Err(ApiError::BadRequest("This file type does not support online editing".to_string()));
    }

    let storage_key = node
        .storage_key
        .clone()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    tokio::fs::write(state.object_storage_directory.join(storage_key), request.content.as_bytes())
        .await
        .map_err(|error| ApiError::Internal(format!("SaveText失败：{}", error)))?;

    let updated_at = now_string();
    let size = request.content.as_bytes().len() as i64;
    let mime = guess_mime_by_name(&node.name).to_string();

    sqlx::query(
        r#"
        UPDATE nodes
        SET size = ?, mime_type = ?, updated_at = ?
        WHERE user_id = ? AND id = ?
        "#,
    )
    .bind(size)
    .bind(mime)
    .bind(&updated_at)
    .bind(&user.id)
    .bind(&node_id)
    .execute(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("更新Text元数据失败：{}", error)))?;

    Ok(Json(TextContentResponse {
        id: node.id,
        name: node.name,
        content: request.content,
        readonly: false,
        encoding: "UTF-8".to_string(),
        updated_at,
    }))
}

async fn create_share(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Json<ShareDto>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let node = get_node_record_by_id(&state.database, &user.id, &node_id).await?;
    if node.parent_id.is_none() {
        return Err(ApiError::BadRequest("Root不能Share".to_string()));
    }

    if let Some(database_row) = sqlx::query(
        r#"
        SELECT token, created_at
        FROM shares
        WHERE owner_id = ? AND node_id = ?
        "#,
    )
    .bind(&user.id)
    .bind(&node_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query share: {}", error)))?
    {
        let token: String = database_row.get("token");
        let created_at: String = database_row.get("created_at");
        let node = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;

        return Ok(Json(ShareDto {
            url: format!("/s/{}", token),
            token,
            node,
            created_at,
        }));
    }

    let token = Uuid::new_v4().simple().to_string();
    let created_at = now_string();

    sqlx::query(
        r#"
        INSERT INTO shares(token, owner_id, node_id, created_at)
        VALUES (?, ?, ?, ?)
        "#,
    )
    .bind(&token)
    .bind(&user.id)
    .bind(&node_id)
    .bind(&created_at)
    .execute(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to create share：{}", error)))?;

    let node = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;

    Ok(Json(ShareDto {
        url: format!("/s/{}", token),
        token,
        node,
        created_at,
    }))
}

async fn get_node_share(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<Json<Option<ShareDto>>> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let _ = get_node_record_by_id(&state.database, &user.id, &node_id).await?;

    let database_row = sqlx::query(
        r#"
        SELECT token, created_at
        FROM shares
        WHERE owner_id = ? AND node_id = ?
        "#,
    )
    .bind(&user.id)
    .bind(&node_id)
    .fetch_optional(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query share: {}", error)))?;

    if let Some(database_row) = database_row {
        let token: String = database_row.get("token");
        let created_at: String = database_row.get("created_at");
        let node = get_node_dto_by_id(&state.database, &user.id, &node_id).await?;

        Ok(Json(Some(ShareDto {
            url: format!("/s/{}", token),
            token,
            node,
            created_at,
        })))
    } else {
        Ok(Json(None))
    }
}

async fn cancel_node_share(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(node_id): Path<String>,
) -> ApiResult<StatusCode> {
    let user = require_user(&state, &jar).await?;

    validate_uuid_text(&node_id, "node_id")?;

    let _ = get_node_record_by_id(&state.database, &user.id, &node_id).await?;

    sqlx::query("DELETE FROM shares WHERE owner_id = ? AND node_id = ?")
        .bind(&user.id)
        .bind(&node_id)
        .execute(&state.database)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to cancel share：{}", error)))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn list_shares(
    State(state): State<AppState>,
    jar: CookieJar,
) -> ApiResult<Json<ListResponse<ShareDto>>> {
    let user = require_user(&state, &jar).await?;

    let database_rows = sqlx::query(
        r#"
        SELECT n.*, s.token AS share_token, s.created_at AS share_created_at
        FROM shares s
        JOIN nodes n ON n.id = s.node_id
        WHERE s.owner_id = ?
        ORDER BY s.created_at DESC
        "#,
    )
    .bind(&user.id)
    .fetch_all(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("获取Share列表失败：{}", error)))?;

    let mut items = Vec::new();

    for database_row in database_rows {
        let token: String = database_row.get("share_token");
        let created_at: String = database_row.get("share_created_at");
        let node = row_to_node_dto(&database_row)?;

        items.push(ShareDto {
            url: format!("/s/{}", token),
            token,
            node,
            created_at,
        });
    }

    Ok(Json(ListResponse { items }))
}

async fn cancel_share_by_token(
    State(state): State<AppState>,
    jar: CookieJar,
    Path(token): Path<String>,
) -> ApiResult<StatusCode> {
    let user = require_user(&state, &jar).await?;

    sqlx::query("DELETE FROM shares WHERE owner_id = ? AND token = ?")
        .bind(&user.id)
        .bind(&token)
        .execute(&state.database)
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to cancel share：{}", error)))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn public_share_info(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> ApiResult<Json<ShareDto>> {
    let (owner_id, node_id, created_at) = get_share_owner_node(&state.database, &token).await?;
    let node = get_node_dto_by_id(&state.database, &owner_id, &node_id).await?;

    Ok(Json(ShareDto {
        token: token.clone(),
        url: format!("/s/{}", token),
        node,
        created_at,
    }))
}

async fn public_share_download(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> ApiResult<Response> {
    let (owner_id, node_id, _created_at) = get_share_owner_node(&state.database, &token).await?;

    download_node_for_owner(&state, &owner_id, &node_id).await
}

async fn public_share_preview(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let (owner_id, node_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    let node = get_node_record_by_id(&state.database, &owner_id, &node_id).await?;

    preview_file_record(&state, &node, &headers).await
}

async fn public_share_text(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> ApiResult<Json<TextContentResponse>> {
    let (owner_id, node_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    let node = get_node_record_by_id(&state.database, &owner_id, &node_id).await?;
    let (content, encoding) = read_text_content(&state, &node).await?;

    Ok(Json(TextContentResponse {
        id: node.id,
        name: node.name,
        content,
        readonly: true,
        encoding,
        updated_at: node.updated_at,
    }))
}

// 公开Share目录的只读子目录列表。
async fn public_share_children(
    State(state): State<AppState>,
    Path((token, folder_id)): Path<(String, String)>,
) -> ApiResult<Json<ListResponse<NodeDto>>> {
    validate_uuid_text(&folder_id, "folder_id")?;

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    ensure_share_node_access(&state.database, &owner_id, &share_root_id, &folder_id).await?;

    let folder = get_node_record_by_id(&state.database, &owner_id, &folder_id).await?;
    if folder.kind != NodeKind::Folder {
        return Err(ApiError::BadRequest("Target is not a folder".to_string()));
    }

    let database_rows = sqlx::query(
        r#"
        SELECT n.*, s.token AS share_token
        FROM nodes n
        LEFT JOIN shares s ON s.node_id = n.id AND s.owner_id = n.user_id
        WHERE n.user_id = ? AND n.parent_id = ?
        ORDER BY CASE n.kind WHEN 'folder' THEN 0 ELSE 1 END, lower(n.name) ASC
        "#,
    )
    .bind(&owner_id)
    .bind(&folder_id)
    .fetch_all(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("读取ShareFolder失败：{}", error)))?;

    let mut items = Vec::new();
    for database_row in database_rows {
        items.push(row_to_node_dto(&database_row)?);
    }

    Ok(Json(ListResponse { items }))
}

async fn public_share_breadcrumbs(
    State(state): State<AppState>,
    Path((token, node_id)): Path<(String, String)>,
) -> ApiResult<Json<BreadcrumbResponse>> {
    validate_uuid_text(&node_id, "node_id")?;

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    ensure_share_node_access(&state.database, &owner_id, &share_root_id, &node_id).await?;

    let all = build_breadcrumbs(&state.database, &owner_id, &node_id).await?;
    let start = all
        .iter()
        .position(|item| item.id == share_root_id)
        .unwrap_or(0);
    let items = all.into_iter().skip(start).collect();

    Ok(Json(BreadcrumbResponse { items }))
}

async fn public_share_node_download(
    State(state): State<AppState>,
    Path((token, node_id)): Path<(String, String)>,
) -> ApiResult<Response> {
    validate_uuid_text(&node_id, "node_id")?;

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    ensure_share_node_access(&state.database, &owner_id, &share_root_id, &node_id).await?;

    download_node_for_owner(&state, &owner_id, &node_id).await
}

async fn public_share_selected_download(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Json(request): Json<SelectedDownloadRequest>,
) -> ApiResult<Response> {
    if request.node_ids.is_empty() {
        return Err(ApiError::BadRequest("Please select files or folders to download".to_string()));
    }

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;

    for node_id in &request.node_ids {
        validate_uuid_text(node_id, "node_id")?;
        ensure_share_node_access(&state.database, &owner_id, &share_root_id, node_id).await?;
    }

    download_selected_nodes_for_owner(
        &state,
        &owner_id,
        &request.node_ids,
        "shared-selected.7z",
    )
    .await
}

// 公开ShareFile的只读在线预览。
async fn public_share_node_preview(
    State(state): State<AppState>,
    Path((token, node_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    validate_uuid_text(&node_id, "node_id")?;

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    ensure_share_node_access(&state.database, &owner_id, &share_root_id, &node_id).await?;

    let node = get_node_record_by_id(&state.database, &owner_id, &node_id).await?;
    preview_file_record(&state, &node, &headers).await
}

// 公开ShareTextFile的只读View。
async fn public_share_node_text(
    State(state): State<AppState>,
    Path((token, node_id)): Path<(String, String)>,
) -> ApiResult<Json<TextContentResponse>> {
    validate_uuid_text(&node_id, "node_id")?;

    let (owner_id, share_root_id, _created_at) = get_share_owner_node(&state.database, &token).await?;
    ensure_share_node_access(&state.database, &owner_id, &share_root_id, &node_id).await?;

    let node = get_node_record_by_id(&state.database, &owner_id, &node_id).await?;
    let (content, encoding) = read_text_content(&state, &node).await?;

    Ok(Json(TextContentResponse {
        id: node.id,
        name: node.name,
        content,
        readonly: true,
        encoding,
        updated_at: node.updated_at,
    }))
}


async fn download_node_for_owner(
    state: &AppState,
    owner_id: &str,
    node_id: &str,
) -> ApiResult<Response> {
    let node = get_node_record_by_id(&state.database, owner_id, node_id).await?;

    match node.kind {
        NodeKind::File => download_file_record(state, &node).await,
        NodeKind::Folder => download_folder_as_7z(state, owner_id, &node).await,
    }
}

async fn download_file_record(state: &AppState, node: &NodeRecord) -> ApiResult<Response> {
    let storage_key = node
        .storage_key
        .clone()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    let bytes = tokio::fs::read(state.object_storage_directory.join(storage_key))
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to read file: {}", error)))?;

    build_file_response(
        bytes,
        &node.name,
        node.mime_type
            .as_deref()
            .unwrap_or_else(|| guess_mime_by_name(&node.name)),
        "attachment",
        None,
    )
}

async fn preview_file_record(
    state: &AppState,
    node: &NodeRecord,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    if node.kind != NodeKind::File {
        return Err(ApiError::BadRequest("只有File可以在线预览".to_string()));
    }

    if !is_previewable_name(&node.name) {
        return Err(ApiError::BadRequest("该FileType暂不支持在线预览".to_string()));
    }

    let storage_key = node
        .storage_key
        .clone()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    let bytes = tokio::fs::read(state.object_storage_directory.join(storage_key))
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to read file: {}", error)))?;

    build_file_response(
        bytes,
        &node.name,
        node.mime_type
            .as_deref()
            .unwrap_or_else(|| guess_mime_by_name(&node.name)),
        "inline",
        Some(headers),
    )
}

async fn read_text_content(state: &AppState, node: &NodeRecord) -> ApiResult<(String, String)> {
    if node.kind != NodeKind::File || !is_editable_text_name(&node.name) {
        return Err(ApiError::BadRequest("该FileType不支持TextView".to_string()));
    }

    if node.size as usize > MAX_TEXT_VIEW_BYTES {
        return Err(ApiError::BadRequest("File太大，暂不支持Online Preview".to_string()));
    }

    let storage_key = node
        .storage_key
        .clone()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    let bytes = tokio::fs::read(state.object_storage_directory.join(storage_key))
        .await
        .map_err(|error| ApiError::Internal(format!("读取Text失败：{}", error)))?;

    let mut detector = EncodingDetector::new(Iso2022JpDetection::Deny);
    detector.feed(&bytes, true);
    let encoding = detector.guess(None, Utf8Detection::Allow);
    let (decoded, _actual_encoding, had_errors) = encoding.decode(&bytes);

    if had_errors {
        return Err(ApiError::BadRequest(format!(
            "Text编码检测为 {}，但解码时出现错误",
            encoding.name()
        )));
    }

    Ok((decoded.into_owned(), encoding.name().to_string()))
}

fn build_file_response(
    bytes: Vec<u8>,
    file_name: &str,
    content_type: &str,
    disposition: &str,
    headers: Option<&HeaderMap>,
) -> ApiResult<Response> {
    let safe_name = safe_ascii_filename(file_name);
    let total_len = bytes.len() as u64;

    if let Some(headers) = headers {
        if let Some((start, end)) = parse_range_header(headers, total_len) {
            let part = bytes[start as usize..=end as usize].to_vec();
            let part_len = part.len();

            return Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::ACCEPT_RANGES, "bytes")
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {}-{}/{}", start, end, total_len),
                )
                .header(header::CONTENT_LENGTH, part_len.to_string())
                .header(
                    header::CONTENT_DISPOSITION,
                    format!("{}; filename=\"{}\"", disposition, safe_name),
                )
                .body(Body::from(part))
                .map_err(|error| ApiError::Internal(format!("构建 Range 响应失败：{}", error)));
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .header(
            header::CONTENT_DISPOSITION,
            format!("{}; filename=\"{}\"", disposition, safe_name),
        )
        .body(Body::from(bytes))
        .map_err(|error| ApiError::Internal(format!("构建Download响应失败：{}", error)))
}

fn parse_range_header(headers: &HeaderMap, len: u64) -> Option<(u64, u64)> {
    if len == 0 {
        return None;
    }

    let value = headers.get(header::RANGE)?.to_str().ok()?;

    if !value.starts_with("bytes=") {
        return None;
    }

    let range = &value[6..];
    let (start_text, end_text) = range.split_once('-')?;

    if start_text.is_empty() {
        let suffix_len = end_text.parse::<u64>().ok()?;
        if suffix_len == 0 {
            return None;
        }

        let start = len.saturating_sub(suffix_len);
        let end = len - 1;

        return Some((start, end));
    }

    let start = start_text.parse::<u64>().ok()?;

    if start >= len {
        return None;
    }

    let end = if end_text.is_empty() {
        len - 1
    } else {
        end_text.parse::<u64>().ok()?.min(len - 1)
    };

    if start > end {
        return None;
    }

    Some((start, end))
}

// 将Folder压缩为 7z 后Download。
async fn download_folder_as_7z(
    state: &AppState,
    owner_id: &str,
    folder: &NodeRecord,
) -> ApiResult<Response> {
    let all_nodes = load_all_nodes(&state.database, owner_id).await?;
    let entries = build_archive_entries_for_selected_nodes(
        &all_nodes,
        &[folder.id.clone()],
        &state.object_storage_directory,
    )?;

    let archive_file_name = format!("{}.7z", folder.name);
    create_and_download_7z_archive(state, entries, &archive_file_name).await
}

async fn download_selected_nodes_for_owner(
    state: &AppState,
    owner_id: &str,
    node_ids: &[String],
    archive_file_name: &str,
) -> ApiResult<Response> {
    let all_nodes = load_all_nodes(&state.database, owner_id).await?;
    let entries = build_archive_entries_for_selected_nodes(
        &all_nodes,
        node_ids,
        &state.object_storage_directory,
    )?;

    create_and_download_7z_archive(state, entries, archive_file_name).await
}

async fn create_and_download_7z_archive(
    state: &AppState,
    entries: Vec<ArchiveEntry>,
    archive_file_name: &str,
) -> ApiResult<Response> {
    let archive_path = state.temporary_directory.join(format!("{}.7z", Uuid::new_v4()));
    let archive_path_for_task = archive_path.clone();
    let staging_directory = state.temporary_directory.join(format!("stage-{}", Uuid::new_v4()));
    let staging_directory_for_task = staging_directory.clone();

    task::spawn_blocking(move || create_7z_file(archive_path_for_task, staging_directory_for_task, entries))
        .await
        .map_err(|error| ApiError::Internal(format!("压缩任务失败：{}", error)))?
        .map_err(ApiError::Internal)?;

    let bytes = tokio::fs::read(&archive_path)
        .await
        .map_err(|error| ApiError::Internal(format!("读取 7z 压缩包失败：{}", error)))?;

    let _ = tokio::fs::remove_file(&archive_path).await;
    let _ = tokio::fs::remove_dir_all(&staging_directory).await;

    build_file_response(bytes, archive_file_name, "application/x-7z-compressed", "attachment", None)
}

#[derive(Debug, Clone)]
struct ArchiveEntry {
    relative_path: String,
    source_path: Option<PathBuf>,
    is_dir: bool,
}

fn build_archive_entries_for_selected_nodes(
    all_nodes: &[NodeRecord],
    selected_node_ids: &[String],
    object_storage_directory: &PathBuf,
) -> ApiResult<Vec<ArchiveEntry>> {
    let mut children_by_parent: HashMap<Option<String>, Vec<NodeRecord>> = HashMap::new();

    for node in all_nodes.iter().cloned() {
        children_by_parent
            .entry(node.parent_id.clone())
            .or_default()
            .push(node);
    }

    let mut entries = Vec::new();
    let mut used_root_names = HashSet::new();

    for selected_node_id in selected_node_ids {
        let node = all_nodes
            .iter()
            .find(|candidate| candidate.id == *selected_node_id)
            .ok_or_else(|| ApiError::NotFound("Node does not exist".to_string()))?;

        let mut root_name = sanitize_archive_component(&node.name);
        if used_root_names.contains(&root_name) {
            let short_id = node.id.chars().take(8).collect::<String>();
            root_name = format!("{}-{}", root_name, short_id);
        }
        used_root_names.insert(root_name.clone());

        collect_archive_entries_recursive(
            node,
            &root_name,
            &children_by_parent,
            object_storage_directory,
            &mut entries,
        )?;
    }

    Ok(entries)
}

fn collect_archive_entries_recursive(
    node: &NodeRecord,
    relative_path: &str,
    children_by_parent: &HashMap<Option<String>, Vec<NodeRecord>>,
    object_storage_directory: &PathBuf,
    entries: &mut Vec<ArchiveEntry>,
) -> ApiResult<()> {
    match &node.kind {
        NodeKind::Folder => {
            let directory_path = ensure_trailing_slash(relative_path);

            entries.push(ArchiveEntry {
                relative_path: directory_path.clone(),
                source_path: None,
                is_dir: true,
            });

            let children = children_by_parent
                .get(&Some(node.id.clone()))
                .cloned()
                .unwrap_or_default();

            for child in children {
                let child_name = sanitize_archive_component(&child.name);
                let child_path = format!("{}{}", directory_path, child_name);

                collect_archive_entries_recursive(
                    &child,
                    &child_path,
                    children_by_parent,
                    object_storage_directory,
                    entries,
                )?;
            }
        }
        NodeKind::File => {
            let storage_key = node
                .storage_key
                .clone()
                .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

            entries.push(ArchiveEntry {
                relative_path: relative_path.to_string(),
                source_path: Some(object_storage_directory.join(storage_key)),
                is_dir: false,
            });
        }
    }

    Ok(())
}

fn create_7z_file(
    archive_path: PathBuf,
    staging_directory: PathBuf,
    entries: Vec<ArchiveEntry>,
) -> Result<(), String> {
    if staging_directory.exists() {
        fs::remove_dir_all(&staging_directory)
            .map_err(|error| format!("清理临时目录失败：{}", error))?;
    }

    fs::create_dir_all(&staging_directory)
        .map_err(|error| format!("Create临时目录失败：{}", error))?;

    for entry in entries {
        let target_path = staging_directory.join(StdPath::new(&entry.relative_path));

        if entry.is_dir {
            fs::create_dir_all(&target_path)
                .map_err(|error| format!("Create临时目录条目失败：{}", error))?;
        } else {
            let source_path = entry
                .source_path
                .ok_or_else(|| "7z File条目缺少 source_path".to_string())?;

            if let Some(parent_directory) = target_path.parent() {
                fs::create_dir_all(parent_directory)
                    .map_err(|error| format!("Create临时File父目录失败：{}", error))?;
            }

            fs::copy(&source_path, &target_path)
                .map_err(|error| format!("复制源File到临时目录失败：{}", error))?;
        }
    }

    let mut sevenz_writer = SevenZWriter::create(&archive_path)
        .map_err(|error| format!("Create 7z File失败：{}", error))?;

    sevenz_writer.set_content_methods(vec![
        lzma::LZMA2Options::with_preset(9).into(),
    ]);

    sevenz_writer
        .push_source_path(&staging_directory, |_| true)
        .map_err(|error| format!("写入 7z File失败：{}", error))?;

    sevenz_writer
        .finish()
        .map_err(|error| format!("完成 7z Compression failed: {}", error))?;

    fs::remove_dir_all(&staging_directory)
        .map_err(|error| format!("Delete临时目录失败：{}", error))?;

    Ok(())
}

async fn create_session_and_cookie(
    state: &AppState,
    jar: CookieJar,
    user_id: &str,
) -> ApiResult<(CookieJar, UserDto)> {
    let token = Uuid::new_v4().simple().to_string();
    let now = now_string();

    sqlx::query(
        r#"
        INSERT INTO sessions(token, user_id, created_at)
        VALUES (?, ?, ?)
        "#,
    )
    .bind(&token)
    .bind(user_id)
    .bind(&now)
    .execute(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("CreateLog in会话失败：{}", error)))?;

    let cookie = Cookie::build(("sid", token))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .build();

    let user = get_user_by_id(&state.database, user_id).await?;

    Ok((jar.add(cookie), user))
}

async fn require_user(state: &AppState, jar: &CookieJar) -> ApiResult<UserDto> {
    let token = jar
        .get("sid")
        .map(|cookie| cookie.value().to_string())
        .ok_or_else(|| ApiError::Unauthorized("请先Log in".to_string()))?;

    let database_row = sqlx::query(
        r#"
        SELECT u.id, u.username, u.root_node_id
        FROM sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.token = ?
        "#,
    )
    .bind(token)
    .fetch_optional(&state.database)
    .await
    .map_err(|error| ApiError::Internal(format!("查询Log in会话失败：{}", error)))?
    .ok_or_else(|| ApiError::Unauthorized("Log in状态已失效，请重新Log in".to_string()))?;

    Ok(UserDto {
        id: database_row.get("id"),
        username: database_row.get("username"),
        root_node_id: database_row.get("root_node_id"),
    })
}

async fn get_user_by_id(database: &SqlitePool, user_id: &str) -> ApiResult<UserDto> {
    let database_row = sqlx::query(
        r#"
        SELECT id, username, root_node_id
        FROM users
        WHERE id = ?
        "#,
    )
    .bind(user_id)
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query user: {}", error)))?
    .ok_or_else(|| ApiError::NotFound("用户不存在".to_string()))?;

    Ok(UserDto {
        id: database_row.get("id"),
        username: database_row.get("username"),
        root_node_id: database_row.get("root_node_id"),
    })
}

async fn get_node_record_by_id(
    database: &SqlitePool,
    user_id: &str,
    node_id: &str,
) -> ApiResult<NodeRecord> {
    let database_row = sqlx::query(
        r#"
        SELECT *
        FROM nodes
        WHERE user_id = ? AND id = ?
        "#,
    )
    .bind(user_id)
    .bind(node_id)
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query node: {}", error)))?
    .ok_or_else(|| ApiError::NotFound("Node does not exist".to_string()))?;

    row_to_node_record(&database_row)
}

async fn get_node_dto_by_id(
    database: &SqlitePool,
    user_id: &str,
    node_id: &str,
) -> ApiResult<NodeDto> {
    let database_row = sqlx::query(
        r#"
        SELECT n.*, s.token AS share_token
        FROM nodes n
        LEFT JOIN shares s ON s.node_id = n.id AND s.owner_id = n.user_id
        WHERE n.user_id = ? AND n.id = ?
        "#,
    )
    .bind(user_id)
    .bind(node_id)
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query node: {}", error)))?
    .ok_or_else(|| ApiError::NotFound("Node does not exist".to_string()))?;

    row_to_node_dto(&database_row)
}

fn row_to_node_record(database_row: &sqlx::sqlite::SqliteRow) -> ApiResult<NodeRecord> {
    let kind_str: String = database_row.get("kind");

    Ok(NodeRecord {
        id: database_row.get("id"),
        parent_id: database_row.get("parent_id"),
        name: database_row.get("name"),
        kind: NodeKind::from_db_str(&kind_str)?,
        size: database_row.get("size"),
        mime_type: database_row.get("mime_type"),
        storage_key: database_row.get("storage_key"),
        created_at: database_row.get("created_at"),
        updated_at: database_row.get("updated_at"),
    })
}

fn row_to_node_dto(database_row: &sqlx::sqlite::SqliteRow) -> ApiResult<NodeDto> {
    let record = row_to_node_record(database_row)?;
    let share_token: Option<String> = database_row.try_get::<Option<String>, _>("share_token").unwrap_or(None);

    let preview_kind = preview_kind_by_name(&record.name).to_string();
    let editable_text = record.kind == NodeKind::File && is_editable_text_name(&record.name);

    Ok(NodeDto {
        id: record.id,
        parent_id: record.parent_id,
        name: record.name,
        kind: record.kind,
        size: record.size,
        mime_type: record.mime_type,
        created_at: record.created_at,
        updated_at: record.updated_at,
        shared: share_token.is_some(),
        share_token,
        preview_kind,
        editable_text,
    })
}

async fn load_all_nodes(database: &SqlitePool, user_id: &str) -> ApiResult<Vec<NodeRecord>> {
    let database_rows = sqlx::query(
        r#"
        SELECT *
        FROM nodes
        WHERE user_id = ?
        ORDER BY CASE kind WHEN 'folder' THEN 0 ELSE 1 END, lower(name) ASC
        "#,
    )
    .bind(user_id)
    .fetch_all(database)
    .await
    .map_err(|error| ApiError::Internal(format!("加载节点失败：{}", error)))?;

    let mut nodes = Vec::new();

    for database_row in database_rows {
        nodes.push(row_to_node_record(&database_row)?);
    }

    Ok(nodes)
}

fn collect_subtree_ids(all_nodes: &[NodeRecord], root_id: &str) -> Vec<String> {
    let mut children_by_parent: HashMap<String, Vec<String>> = HashMap::new();

    for node in all_nodes {
        if let Some(parent_id) = &node.parent_id {
            children_by_parent
                .entry(parent_id.clone())
                .or_default()
                .push(node.id.clone());
        }
    }

    let mut result = Vec::new();
    let mut stack = vec![root_id.to_string()];

    while let Some(id) = stack.pop() {
        result.push(id.clone());

        if let Some(children) = children_by_parent.get(&id) {
            for child_id in children {
                stack.push(child_id.clone());
            }
        }
    }

    result
}

async fn build_breadcrumbs(
    database: &SqlitePool,
    user_id: &str,
    node_id: &str,
) -> ApiResult<Vec<BreadcrumbItem>> {
    let mut items = Vec::new();
    let mut current_id = Some(node_id.to_string());

    while let Some(id) = current_id {
        let node = get_node_record_by_id(database, user_id, &id).await?;

        items.push(BreadcrumbItem {
            id: node.id.clone(),
            name: node.name.clone(),
        });

        current_id = node.parent_id;
    }

    items.reverse();

    Ok(items)
}


enum UploadNameResolution {
    UseName(String),
    AlreadyExists(String),
}

enum MoveNameResolution {
    UseName(String),
    MergeInto(String),
}

async fn resolve_upload_file_name(
    state: &AppState,
    user_id: &str,
    parent_id: &str,
    requested_name: &str,
    uploaded_path: &StdPath,
    uploaded_size: i64,
) -> ApiResult<UploadNameResolution> {
    let clean_name = requested_name.trim().to_string();

    if let Some(existing_node) = find_name_conflict(
        &state.database,
        user_id,
        parent_id,
        &clean_name,
        None,
    )
    .await?
    {
        if existing_node.kind == NodeKind::File {
            let same_content = same_uploaded_and_existing_content(
                state,
                &existing_node,
                uploaded_path,
                uploaded_size,
            )
            .await?;

            if same_content {
                return Ok(UploadNameResolution::AlreadyExists(existing_node.id));
            }
        }

        let renamed = next_available_timestamped_name(
            &state.database,
            user_id,
            parent_id,
            &clean_name,
            None,
        )
        .await?;

        return Ok(UploadNameResolution::UseName(renamed));
    }

    Ok(UploadNameResolution::UseName(clean_name))
}

async fn move_node_record_to_parent(
    state: &AppState,
    user_id: &str,
    node: &NodeRecord,
    new_parent_id: &str,
) -> ApiResult<NodeDto> {
    let resolution = resolve_move_name(
        state,
        user_id,
        node,
        new_parent_id,
    )
    .await?;

    match resolution {
        MoveNameResolution::UseName(final_name) => {
            sqlx::query(
                r#"
                UPDATE nodes
                SET parent_id = ?, name = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                "#,
            )
            .bind(new_parent_id)
            .bind(final_name.trim())
            .bind(now_string())
            .bind(&node.id)
            .bind(user_id)
            .execute(&state.database)
            .await
            .map_err(|error| db_conflict_or_internal(error, "Move failed"))?;

            get_node_dto_by_id(&state.database, user_id, &node.id).await
        }
        MoveNameResolution::MergeInto(existing_node_id) => {
            sqlx::query("DELETE FROM nodes WHERE id = ? AND user_id = ?")
                .bind(&node.id)
                .bind(user_id)
                .execute(&state.database)
                .await
                .map_err(|error| ApiError::Internal(format!("合并同名File失败：{}", error)))?;

            if let Some(storage_key) = &node.storage_key {
                let _ = tokio::fs::remove_file(state.object_storage_directory.join(storage_key)).await;
            }

            get_node_dto_by_id(&state.database, user_id, &existing_node_id).await
        }
    }
}

async fn resolve_move_name(
    state: &AppState,
    user_id: &str,
    node: &NodeRecord,
    new_parent_id: &str,
) -> ApiResult<MoveNameResolution> {
    if let Some(existing_node) = find_name_conflict(
        &state.database,
        user_id,
        new_parent_id,
        &node.name,
        Some(&node.id),
    )
    .await?
    {
        if node.kind == NodeKind::File && existing_node.kind == NodeKind::File {
            let same_content = same_file_content(state, node, &existing_node).await?;
            if same_content {
                return Ok(MoveNameResolution::MergeInto(existing_node.id));
            }
        }

        let renamed = next_available_timestamped_name(
            &state.database,
            user_id,
            new_parent_id,
            &node.name,
            Some(&node.id),
        )
        .await?;

        return Ok(MoveNameResolution::UseName(renamed));
    }

    Ok(MoveNameResolution::UseName(node.name.clone()))
}

async fn delete_nodes_for_owner(
    state: &AppState,
    user_id: &str,
    node_ids: &[String],
) -> ApiResult<()> {
    let all_nodes = load_all_nodes(&state.database, user_id).await?;
    let mut subtree_set: HashSet<String> = HashSet::new();

    for node_id in node_ids {
        let node = all_nodes
            .iter()
            .find(|candidate| candidate.id == *node_id)
            .ok_or_else(|| ApiError::NotFound("Node does not exist".to_string()))?;

        if node.parent_id.is_none() {
            return Err(ApiError::BadRequest("不能DeleteRoot".to_string()));
        }

        for id in collect_subtree_ids(&all_nodes, node_id) {
            subtree_set.insert(id);
        }
    }

    let mut storage_keys = Vec::new();
    for item in &all_nodes {
        if subtree_set.contains(&item.id) {
            if let Some(storage_key) = &item.storage_key {
                storage_keys.push(storage_key.clone());
            }
        }
    }

    let mut transaction = state
        .database
        .begin()
        .await
        .map_err(|error| ApiError::Internal(format!("Failed to start transaction: {}", error)))?;

    for id in &subtree_set {
        sqlx::query("DELETE FROM nodes WHERE id = ? AND user_id = ?")
            .bind(id)
            .bind(user_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| ApiError::Internal(format!("Failed to delete node: {}", error)))?;
    }

    transaction
        .commit()
        .await
        .map_err(|error| ApiError::Internal(format!("提交Delete failed：{}", error)))?;

    for storage_key in storage_keys {
        let _ = tokio::fs::remove_file(state.object_storage_directory.join(storage_key)).await;
    }

    Ok(())
}

async fn find_name_conflict(
    database: &SqlitePool,
    user_id: &str,
    parent_id: &str,
    name: &str,
    except_id: Option<&str>,
) -> ApiResult<Option<NodeRecord>> {
    let database_row = sqlx::query(
        r#"
        SELECT *
        FROM nodes
        WHERE user_id = ? AND parent_id = ? AND lower(name) = lower(?)
        "#,
    )
    .bind(user_id)
    .bind(parent_id)
    .bind(name.trim())
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("检查重名失败：{}", error)))?;

    if let Some(database_row) = database_row {
        let found_id: String = database_row.get("id");
        if Some(found_id.as_str()) == except_id {
            return Ok(None);
        }

        return row_to_node_record(&database_row).map(Some);
    }

    Ok(None)
}

async fn next_available_timestamped_name(
    database: &SqlitePool,
    user_id: &str,
    parent_id: &str,
    requested_name: &str,
    except_id: Option<&str>,
) -> ApiResult<String> {
    let timestamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();

    for attempt in 0..1000 {
        let candidate = name_with_timestamp_suffix(requested_name, &timestamp, attempt);
        validate_node_name(&candidate)?;

        if find_name_conflict(database, user_id, parent_id, &candidate, except_id)
            .await?
            .is_none()
        {
            return Ok(candidate);
        }
    }

    Err(ApiError::Conflict(
        "无法自动生成不重名的名称，请稍后重试".to_string(),
    ))
}

async fn same_uploaded_and_existing_content(
    state: &AppState,
    existing_node: &NodeRecord,
    uploaded_path: &StdPath,
    uploaded_size: i64,
) -> ApiResult<bool> {
    if existing_node.kind != NodeKind::File || existing_node.size != uploaded_size {
        return Ok(false);
    }

    let existing_storage_key = existing_node
        .storage_key
        .as_deref()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    let existing_hash = sha256_path(&state.object_storage_directory.join(existing_storage_key)).await?;
    let uploaded_hash = sha256_path(uploaded_path).await?;

    Ok(existing_hash == uploaded_hash)
}

async fn same_file_content(
    state: &AppState,
    left: &NodeRecord,
    right: &NodeRecord,
) -> ApiResult<bool> {
    if left.kind != NodeKind::File || right.kind != NodeKind::File || left.size != right.size {
        return Ok(false);
    }

    let left_storage_key = left
        .storage_key
        .as_deref()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;
    let right_storage_key = right
        .storage_key
        .as_deref()
        .ok_or_else(|| ApiError::Internal("File缺少 storage_key".to_string()))?;

    let left_hash = sha256_path(&state.object_storage_directory.join(left_storage_key)).await?;
    let right_hash = sha256_path(&state.object_storage_directory.join(right_storage_key)).await?;

    Ok(left_hash == right_hash)
}

async fn sha256_path(path: &StdPath) -> ApiResult<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| ApiError::Internal(format!("OpenFile计算哈希失败：{}", error)))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];

    loop {
        let read_bytes = file
            .read(&mut buffer)
            .await
            .map_err(|error| ApiError::Internal(format!("读取File计算哈希失败：{}", error)))?;

        if read_bytes == 0 {
            break;
        }

        hasher.update(&buffer[..read_bytes]);
    }

    let digest = hasher.finalize();
    Ok(digest.iter().map(|byte| format!("{:02x}", byte)).collect())
}

fn name_with_timestamp_suffix(name: &str, timestamp: &str, attempt: usize) -> String {
    let name = name.trim();
    let suffix = if attempt == 0 {
        format!("-{}", timestamp)
    } else {
        format!("-{}-{}", timestamp, attempt + 1)
    };

    let (mut stem, mut extension) = split_file_name_extension(name);

    if suffix.len() + extension.len() + 1 > 255 {
        stem = name;
        extension = "";
    }

    let max_stem_bytes = 255usize.saturating_sub(suffix.len() + extension.len()).max(1);
    let mut shortened_stem = truncate_utf8_boundary(stem, max_stem_bytes);

    if shortened_stem.trim().is_empty() {
        shortened_stem = "file".to_string();
    }

    format!("{}{}{}", shortened_stem, suffix, extension)
}

fn split_file_name_extension(name: &str) -> (&str, &str) {
    if let Some(dot_index) = name.rfind('.') {
        if dot_index > 0 && dot_index < name.len() - 1 {
            return (&name[..dot_index], &name[dot_index..]);
        }
    }

    (name, "")
}

fn truncate_utf8_boundary(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }

    value[..end].to_string()
}

async fn ensure_no_name_conflict(
    database: &SqlitePool,
    user_id: &str,
    parent_id: &str,
    name: &str,
    except_id: Option<&str>,
) -> ApiResult<()> {
    let database_row = sqlx::query(
        r#"
        SELECT id
        FROM nodes
        WHERE user_id = ? AND parent_id = ? AND lower(name) = lower(?)
        "#,
    )
    .bind(user_id)
    .bind(parent_id)
    .bind(name.trim())
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("检查重名失败：{}", error)))?;

    if let Some(database_row) = database_row {
        let found_id: String = database_row.get("id");

        if Some(found_id.as_str()) != except_id {
            return Err(ApiError::Conflict(
                "同一目录下已经存在同名File或Folder".to_string(),
            ));
        }
    }

    Ok(())
}

async fn ensure_not_move_folder_into_descendant(
    database: &SqlitePool,
    user_id: &str,
    moving_folder_id: &str,
    target_parent_id: &str,
) -> ApiResult<()> {
    if moving_folder_id == target_parent_id {
        return Err(ApiError::BadRequest(
            "不能把FolderMove到自己里面".to_string(),
        ));
    }

    let mut current = Some(target_parent_id.to_string());

    while let Some(id) = current {
        if id == moving_folder_id {
            return Err(ApiError::BadRequest(
                "不能把FolderMove到自己的子目录中".to_string(),
            ));
        }

        let database_row = sqlx::query(
            r#"
            SELECT parent_id
            FROM nodes
            WHERE user_id = ? AND id = ?
            "#,
        )
        .bind(user_id)
        .bind(&id)
        .fetch_optional(database)
        .await
        .map_err(|error| ApiError::Internal(format!("检查Move目标失败：{}", error)))?;

        current = database_row.and_then(|parent_row| parent_row.get::<Option<String>, _>("parent_id"));
    }

    Ok(())
}

async fn get_share_owner_node(
    database: &SqlitePool,
    token: &str,
) -> ApiResult<(String, String, String)> {
    let database_row = sqlx::query(
        r#"
        SELECT owner_id, node_id, created_at
        FROM shares
        WHERE token = ?
        "#,
    )
    .bind(token)
    .fetch_optional(database)
    .await
    .map_err(|error| ApiError::Internal(format!("Failed to query share: {}", error)))?
    .ok_or_else(|| ApiError::NotFound("Share does not exist或已Cancel".to_string()))?;

    Ok((
        database_row.get("owner_id"),
        database_row.get("node_id"),
        database_row.get("created_at"),
    ))
}

async fn ensure_share_node_access(
    database: &SqlitePool,
    owner_id: &str,
    share_root_id: &str,
    requested_node_id: &str,
) -> ApiResult<()> {
    let mut current = Some(requested_node_id.to_string());

    while let Some(id) = current {
        if id == share_root_id {
            return Ok(());
        }

        let database_row = sqlx::query(
            r#"
            SELECT parent_id
            FROM nodes
            WHERE user_id = ? AND id = ?
            "#,
        )
        .bind(owner_id)
        .bind(&id)
        .fetch_optional(database)
        .await
        .map_err(|error| ApiError::Internal(format!("检查Share访问范围失败：{}", error)))?
        .ok_or_else(|| ApiError::NotFound("Node does not exist".to_string()))?;

        current = database_row.get::<Option<String>, _>("parent_id");
    }

    Err(ApiError::NotFound("节点不在Share范围内".to_string()))
}


// 使用 SHA-256 计算Password哈希。
fn hash_password(password: &str) -> ApiResult<String> {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let digest = hasher.finalize();
    Ok(digest.iter().map(|b| format!("{:02x}", b)).collect())
}

fn verify_password(password: &str, password_hash: &str) -> ApiResult<()> {
    if hash_password(password)? == password_hash {
        Ok(())
    } else {
        Err(ApiError::Unauthorized("Incorrect username or password".to_string()))
    }
}

fn validate_username(_username: &str) -> ApiResult<()> {
    Ok(())
}

fn validate_password(_password: &str) -> ApiResult<()> {
    Ok(())
}

fn validate_node_name(name: &str) -> ApiResult<()> {
    let name = name.trim();
    if name.is_empty() {
        Err(ApiError::BadRequest("Name cannot be empty".to_string()))
    } else if name == "." || name == ".." {
        Err(ApiError::BadRequest("名称非法".to_string()))
    } else if name.contains(['/', '\\']) {
        Err(ApiError::BadRequest("名称不能包含斜杠".to_string()))
    } else if name.len() > 255 {
        Err(ApiError::BadRequest("名称不能超过 255 个字符".to_string()))
    } else {
        Ok(())
    }
}


fn validate_uuid_text(value: &str, field: &str) -> ApiResult<()> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| ApiError::BadRequest(format!("{} 必须是 UUID", field)))
}

fn db_conflict_or_internal(error: sqlx::Error, prefix: &str) -> ApiError {
    let text = error.to_string().to_lowercase();

    if text.contains("unique") {
        ApiError::Conflict("同一目录下已经存在同名File或Folder".to_string())
    } else {
        ApiError::Internal(format!("{}：{}", prefix, error))
    }
}

fn now_string() -> String {
    Utc::now().to_rfc3339()
}

fn safe_ascii_filename(name: &str) -> String {
    let mut out = String::new();

    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' || c == ' ' {
            out.push(c);
        } else {
            out.push('_');
        }
    }

    if out.trim().is_empty() {
        "download".to_string()
    } else {
        out
    }
}

fn sanitize_archive_component(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            _ => c,
        })
        .collect::<String>()
}

fn ensure_trailing_slash(value: &str) -> String {
    if value.ends_with('/') {
        value.to_string()
    } else {
        format!("{}/", value)
    }
}

const TEXT_EXTENSIONS: &[&str] = &[
    ".txt", ".md", ".markdown", ".json", ".csv", ".log", ".toml", ".yaml", ".yml",
    ".rs", ".py", ".js", ".ts", ".jsx", ".tsx", ".html", ".css", ".xml",
    ".java", ".c", ".cpp", ".h", ".hpp", ".go", ".php", ".rb", ".sh", ".bat",
    ".ps1", ".ini", ".conf", ".sql", ".vue",
];

const AUDIO_EXTENSIONS: &[&str] = &[".mp3", ".wav", ".ogg", ".flac", ".m4a", ".aac"];
const VIDEO_EXTENSIONS: &[&str] = &[".mp4", ".webm", ".mov", ".mkv", ".avi"];

fn lower_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn has_known_extension(name: &str, extensions: &[&str]) -> bool {
    let name = lower_name(name);
    extensions.iter().any(|ext| name.ends_with(ext))
}

fn is_editable_text_name(name: &str) -> bool {
    has_known_extension(name, TEXT_EXTENSIONS)
}

fn is_audio_name(name: &str) -> bool {
    has_known_extension(name, AUDIO_EXTENSIONS)
}

fn is_video_name(name: &str) -> bool {
    has_known_extension(name, VIDEO_EXTENSIONS)
}

fn is_previewable_name(name: &str) -> bool {
    is_editable_text_name(name) || is_audio_name(name) || is_video_name(name)
}

fn preview_kind_by_name(name: &str) -> &'static str {
    if is_editable_text_name(name) {
        "text"
    } else if is_audio_name(name) {
        "audio"
    } else if is_video_name(name) {
        "video"
    } else {
        "none"
    }
}

fn guess_mime_by_name(name: &str) -> &'static str {
    let name = lower_name(name);

    if name.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if name.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if name.ends_with(".js") {
        "application/javascript; charset=utf-8"
    } else if name.ends_with(".json") {
        "application/json; charset=utf-8"
    } else if name.ends_with(".md") || name.ends_with(".markdown") || name.ends_with(".txt") {
        "text/plain; charset=utf-8"
    } else if name.ends_with(".csv") {
        "text/csv; charset=utf-8"
    } else if has_known_extension(&name, TEXT_EXTENSIONS) {
        "text/plain; charset=utf-8"
    } else if name.ends_with(".mp3") {
        "audio/mpeg"
    } else if name.ends_with(".wav") {
        "audio/wav"
    } else if name.ends_with(".ogg") {
        "audio/ogg"
    } else if name.ends_with(".flac") {
        "audio/flac"
    } else if name.ends_with(".m4a") {
        "audio/mp4"
    } else if name.ends_with(".mp4") {
        "video/mp4"
    } else if name.ends_with(".webm") {
        "video/webm"
    } else if name.ends_with(".mov") {
        "video/quicktime"
    } else {
        "application/octet-stream"
    }
}

const INDEX_HTML: &str = r###"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>RustDrive</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <style>
    :root {
      --orange: #ff5a1f;
      --orange-dark: #d94716;
      --orange-soft: #fff0e8;
      --orange-line: #ffd5c1;
      --green: #e9f6e9;
      --green-strong: rgb(0, 60, 0);
      --green-line: #cce8cc;
      --bg: #fff7f1;
      --panel: #ffffff;
      --text: #2f201b;
      --muted: #81665d;
      --shadow: 0 18px 46px rgba(126, 48, 14, .10);
    }

    * { box-sizing: border-box; }

    html, body { margin: 0; min-height: 100%; }

    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 12% 0%, rgba(255,90,31,.14), transparent 28rem),
        linear-gradient(180deg, #fff9f4 0%, #fff2ea 100%);
      color: var(--text);
    }

    button {
      border: 0;
      border-radius: 12px;
      padding: 9px 14px;
      cursor: pointer;
      font-weight: 800;
      background: var(--orange);
      color: #fff;
      box-shadow: 0 8px 18px rgba(255, 90, 31, .16);
    }

    button:hover { background: var(--orange-dark); }

    button.green {
      background: var(--green);
      color: var(--green-strong);
      border: 1px solid var(--green-line);
      box-shadow: none;
    }

    button.green:hover { background: #dff1df; }

    button.small { padding: 7px 10px; border-radius: 10px; font-size: 13px; }

    input {
      width: 100%;
      border: 1px solid var(--orange-line);
      border-radius: 13px;
      padding: 12px 13px;
      outline: none;
      font: inherit;
      background: white;
    }

    input:focus {
      border-color: var(--orange);
      box-shadow: 0 0 0 4px rgba(255, 90, 31, .11);
    }

    .hidden { display: none !important; }

    .auth-page {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
      background:
        radial-gradient(circle at 18% 0%, rgba(255, 199, 91, .34), transparent 26rem),
        linear-gradient(135deg, #fff4df 0%, #fff0e8 48%, #ffd982 100%);
    }

    .auth-hero { display: none; }

    .auth-card-wrap {
      width: min(480px, 100%);
      display: block;
      padding: 0;
    }

    .auth-card {
      width: 100%;
      background: rgba(255,255,255,.94);
      border: 1px solid #ffc86d;
      border-radius: 24px;
      padding: 34px;
      box-shadow: 0 22px 58px rgba(156, 74, 14, .16);
    }

    .auth-card h2 {
      margin: 0 0 24px;
      font-size: 34px;
      color: var(--orange-dark);
    }

    .auth-card .sub,
    .auth-link { display: none; }

    .field { margin: 20px 0; }
    .field label {
      display: block;
      margin-bottom: 10px;
      font-size: 15px;
      font-weight: 800;
      color: #8a4a00;
    }

    .auth-card input {
      min-height: 54px;
      font-size: 18px;
      border-color: #ffc86d;
      border-radius: 16px;
    }

    .auth-actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 24px;
    }

    .auth-card button {
      min-height: 52px;
      font-size: 17px;
      border-radius: 16px;
      background: var(--orange);
      color: white;
    }

    .auth-card button.green {
      background: #ffd66b;
      color: #6b3b00;
      border: 1px solid #efb93e;
    }

    .auth-card button.green:hover { background: #ffc84d; }

    .application-shell { min-height: 100vh; }

    .topbar {
      height: 40px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 12px;
      background: linear-gradient(90deg, var(--orange), #ff7b4c);
      color: white;
      box-shadow: 0 2px 14px rgba(223, 63, 13, .20);
      position: sticky;
      top: 0;
      z-index: 30;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 8px;
      font-weight: 900;
      white-space: nowrap;
    }

    .brand-mark {
      width: 20px;
      height: 20px;
      border-radius: 6px;
      display: grid;
      place-items: center;
      background: rgba(255,255,255,.24);
      font-size: 13px;
    }

    .crumbs {
      flex: 1;
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 4px;
      overflow-x: auto;
      scrollbar-width: none;
    }

    .crumbs::-webkit-scrollbar { display: none; }

    .crumb {
      height: 22px;
      display: inline-flex;
      align-items: center;
      padding: 0 8px;
      border-radius: 999px;
      background: rgba(255,255,255,.18);
      font-size: 12px;
      color: white;
      cursor: pointer;
      white-space: nowrap;
    }

    .crumb:hover, .crumb.drop-target { background: rgba(255,255,255,.34); }
    .crumb-sep { color: rgba(255,255,255,.68); }

    .userbar { display: flex; align-items: center; gap: 9px; white-space: nowrap; font-size: 13px; }

    .workspace { width: 100%; margin: 0; padding: 10px; }

    .panel {
      width: 100%;
      min-height: calc(100vh - 60px);
      background: transparent;
      border: 0;
      border-radius: 0;
      overflow: visible;
      box-shadow: none;
    }

    .panel-head {
      min-height: 48px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 8px 12px;
      border-bottom: 1px solid var(--orange-line);
      background: rgba(255,255,255,.72);
    }

    .folder-title { min-width: 0; }
    .folder-title h2 { margin: 0; font-size: 18px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .summary { margin-top: 4px; color: var(--muted); font-size: 12px; }

    .toolbar {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .grid {
      min-height: calc(100vh - 50px);
      padding: 14px;
      display: grid;
      grid-template-columns: repeat(auto-fill, 132px);
      align-content: start;
      justify-content: start;
      gap: 12px;
    }

    .card {
      position: relative;
      width: 132px;
      height: 132px;
      padding: 10px;
      border-radius: 14px;
      border: 1px solid #ffe0d1;
      background: white;
      box-shadow: 0 8px 20px rgba(126,48,14,.045);
      transition: transform .12s ease, border-color .12s ease, box-shadow .12s ease, background .12s ease;
      user-select: none;
      cursor: pointer;
    }

    .card:hover {
      transform: translateY(-1px);
      border-color: #ff9b77;
      box-shadow: 0 14px 28px rgba(126,48,14,.09);
    }

    .card.drop-target {
      background: var(--orange-soft);
      outline: 3px solid rgba(255,90,31,.20);
    }

    .card.selected {
      border-color: var(--orange);
      background: var(--orange-soft);
      outline: 2px solid rgba(255,90,31,.18);
    }

    .drag-selection-box {
      position: fixed;
      z-index: 80;
      display: none;
      pointer-events: none;
      border: 1px solid var(--orange);
      background: rgba(255,90,31,.10);
    }

    .file-icon {
      width: 32px;
      height: 32px;
      display: grid;
      place-items: center;
      border-radius: 4px;
      background: var(--orange-soft);
      font-size: 17px;
      margin-bottom: 8px;
    }

    .card.folder .file-icon { background: var(--green); }

    .card-name {
      font-weight: 850;
      font-size: 13px;
      line-height: 1.25;
      word-break: break-word;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
      padding-right: 24px;
    }

    .card-meta { margin-top: 5px; color: var(--muted); font-size: 11px; }

    .badge {
      position: absolute;
      right: 10px;
      bottom: 10px;
      height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      display: inline-flex;
      align-items: center;
      background: var(--green);
      color: var(--green-strong);
      border: 1px solid var(--green-line);
      font-size: 12px;
      font-weight: 850;
    }

    .more {
      position: absolute;
      right: 8px;
      top: 8px;
      width: 26px;
      height: 26px;
      border-radius: 9px;
      display: grid;
      place-items: center;
      background: var(--green);
      color: var(--green-strong);
      border: 1px solid var(--green-line);
      box-shadow: none;
      padding: 0;
      font-size: 15px;
    }

    .empty {
      grid-column: 1 / -1;
      min-height: 260px;
      border: 0;
      border-radius: 0;
      background: transparent;
      display: grid;
      place-items: center;
      text-align: center;
      color: var(--muted);
      padding: 24px;
    }

    .empty h3 { margin: 0 0 8px; color: var(--text); font-size: 20px; }
    .empty p { margin: 0 0 18px; }
    .empty-actions { display: flex; gap: 10px; justify-content: center; flex-wrap: wrap; }

    .selection-bar {
      position: fixed;
      left: 50%;
      bottom: 18px;
      transform: translateX(-50%);
      display: none;
      align-items: center;
      gap: 10px;
      z-index: 90;
      padding: 10px 12px;
      border-radius: 18px;
      background: white;
      border: 1px solid var(--orange-line);
      box-shadow: 0 22px 54px rgba(61,24,9,.18);
    }

    .selection-text {
      font-weight: 850;
      color: var(--text);
      white-space: nowrap;
    }

    .menu {
      position: fixed;
      min-width: 228px;
      display: none;
      z-index: 100;
      background: white;
      border: 1px solid var(--orange-line);
      border-radius: 16px;
      padding: 8px;
      box-shadow: 0 22px 54px rgba(61,24,9,.20);
    }

    .menu-row {
      min-height: 38px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 10px;
      border-radius: 11px;
      font-weight: 800;
      cursor: pointer;
    }

    .menu-row:hover { background: var(--orange-soft); color: var(--orange-dark); }
    .menu-row.green:hover { background: var(--green); color: var(--green-strong); }
    .menu-divider { height: 1px; background: #ffe0d1; margin: 6px 4px; }

    .toast {
      position: fixed;
      right: 18px;
      bottom: 18px;
      max-width: 380px;
      display: none;
      z-index: 200;
      padding: 12px 14px;
      border-radius: 14px;
      background: #2f201b;
      color: white;
      box-shadow: 0 16px 38px rgba(47,32,27,.22);
    }

    @media (max-width: 760px) {
      .auth-page { grid-template-columns: 1fr; }
      .auth-hero { display: none; }
      .workspace { padding: 12px; }
      .panel-head { align-items: flex-start; flex-direction: column; }
      .toolbar { justify-content: flex-start; }
      .grid { grid-template-columns: repeat(auto-fill, minmax(142px, 1fr)); padding: 12px; }
      .userbar span { display: none; }
    }
  </style>
</head>
<body>
  <section id="auth" class="auth-page hidden">
    <div class="auth-card-wrap">
      <div class="auth-card">
        <h2 id="authTitle">Log in</h2>
        <div class="field">
          <label>Username</label>
          <input id="username" autocomplete="username" autofocus />
        </div>
        <div class="field">
          <label>Password</label>
          <input id="password" type="password" autocomplete="current-password" onkeydown="authKey(event)" />
        </div>
        <div class="auth-actions">
          <button id="authMainBtn" onclick="submitAuth()">Log in</button>
          <button class="green" onclick="toggleAuthPage()" id="authSwitchBtn">Register</button>
        </div>
      </div>
    </div>
  </section>

  <section id="application" class="application-shell hidden">
    <nav class="topbar">
      <div class="brand"><span class="brand-mark">云</span><span>橘红Drive</span></div>
      <div id="crumbs" class="crumbs"></div>
      <div class="userbar">
        <span id="whoami"></span>
        <button class="green small" onclick="logout()">Log out</button>
      </div>
    </nav>

    <main class="workspace" id="mainArea">
      <section class="panel">
        <div id="grid" class="grid"></div>
      </section>
    </main>
  </section>

  <input id="fileInput" type="file" multiple hidden onchange="uploadSelectedFiles(this.files)" />
  <input id="folderInput" type="file" multiple webkitdirectory directory hidden onchange="uploadFolderSelected(this.files)" />
  <div id="menu" class="menu"></div>
  <div id="selectionBar" class="selection-bar">
    <span id="selectionText" class="selection-text">Selected 0 项</span>
    <button class="green small" onclick="downloadSelectedNodes()">Download as 7z</button>
    <button class="small" onclick="deleteSelectedNodes()">Delete</button>
    <button class="green small" onclick="clearSelection()">Clear Selection</button>
  </div>
  <div id="dragSelectionBox" class="drag-selection-box"></div>
  <div id="toast" class="toast"></div>

  <script>
    let authMode = location.pathname === "/register" ? "register" : "login";
    let currentUser = null; // 当前Log in用户。
    let rootId = null; // 当前用户Root ID。
    let currentFolderId = null; // 当前正在浏览的目录 ID。
    let currentFolderName = "My Drive";
    let selectedNode = null; // 右键菜单当前选中的节点。
    let selectedNodeIds = new Set(); // 多选File和Folder的 ID 集合。
    let draggedNodeIds = []; // 当前拖拽中的节点 ID 列表。
    let isBoxSelecting = false; // 是否正在使用鼠标框选。
    let boxSelectStartX = 0; // 框选开始位置 X。
    let boxSelectStartY = 0; // 框选开始位置 Y。
    let clickAfterBoxSelect = false; // 防止框选结束后触发普通点击。
    const menu = document.getElementById("menu");

    async function api(path, options = {}) {
      const res = await fetch(path, options);
      if (!res.ok) {
        let msg = res.statusText;
        try { msg = (await res.json()).message || msg; } catch (_) {}
        throw new Error(msg);
      }
      if (res.status === 204) return null;
      return res.json();
    }

    function toast(msg) {
      const element = document.getElementById("toast");
      element.textContent = msg;
      element.style.display = "block";
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(() => element.style.display = "none", 2300);
    }

    async function boot() {
      try {
        const responseData = await api("/api/me");
        await enterApp(responseData.user);
      } catch (_) {
        showAuth();
      }
    }

    function showAuth() {
      document.getElementById("auth").classList.remove("hidden");
      document.getElementById("application").classList.add("hidden");
      applyAuthMode();
    }

    function applyAuthMode() {
      const isRegister = authMode === "register";
      document.getElementById("authTitle").textContent = isRegister ? "Register" : "Log in";
      document.getElementById("authMainBtn").textContent = isRegister ? "Register" : "Log in";
      document.getElementById("authSwitchBtn").textContent = isRegister ? "BackLog in" : "Register";
      history.replaceState(null, "", isRegister ? "/register" : "/login");
    }

    function toggleAuthPage() {
      authMode = authMode === "register" ? "login" : "register";
      applyAuthMode();
    }

    function authKey(event) { if (event.key === "Enter") submitAuth(); }

    async function submitAuth() {
      const username = document.getElementById("username").value;
      const password = document.getElementById("password").value;
      try {
        const responseData = await api(`/api/auth/${authMode}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ username, password }),
        });
        history.replaceState(null, "", "/");
        await enterApp(responseData.user);
      } catch (error) { alert(error.message); }
    }

    async function logout() {
      await api("/api/auth/logout", { method: "POST" });
      location.href = "/login";
    }

    async function enterApp(user) {
      currentUser = user;
      rootId = user.root_node_id;
      currentFolderId = rootId;
      document.getElementById("auth").classList.add("hidden");
      document.getElementById("application").classList.remove("hidden");
      document.getElementById("whoami").textContent = user.username;
      await openFolder(rootId);
    }

    async function openFolder(folderId) {
      currentFolderId = folderId;
      selectedNode = null;
      selectedNodeIds.clear();
      updateSelectionBar();
      await renderCrumbs(folderId);
      const responseData = await api(`/api/nodes/${folderId}/children`);
      renderGrid(responseData.items);
    }

    async function reloadCurrentFolder() {
      if (currentFolderId) await openFolder(currentFolderId);
    }

    async function renderCrumbs(nodeId) {
      const responseData = await api(`/api/nodes/${nodeId}/breadcrumbs`);
      const element = document.getElementById("crumbs");
      element.innerHTML = "";
      responseData.items.forEach((item, itemIndex) => {
        const span = document.createElement("span");
        span.className = "crumb";
        span.textContent = item.name;
        span.onclick = () => openFolder(item.id);
        span.addEventListener("dragover", event => { event.preventDefault(); span.classList.add("drop-target"); });
        span.addEventListener("dragleave", () => span.classList.remove("drop-target"));
        span.addEventListener("drop", async event => {
          event.preventDefault();
          span.classList.remove("drop-target");
          if (draggedNodeIds.length && !draggedNodeIds.includes(item.id)) await moveSelectedNodesTo(item.id);
        });
        element.appendChild(span);
        if (itemIndex < responseData.items.length - 1) {
          const separatorElement = document.createElement("span");
          separatorElement.className = "crumb-sep";
          separatorElement.textContent = "›";
          element.appendChild(separatorElement);
        }
      });
    }

    function renderGrid(items) {
      const grid = document.getElementById("grid");
      grid.innerHTML = "";
      if (!items.length) {
        grid.innerHTML = `
          <div class="empty">
            <div>
              <h3>这个Folder还是空的</h3>
            </div>
          </div>`;
        return;
      }

      items.forEach(node => {
        const card = document.createElement("div");
        card.className = `card ${node.kind}` + (selectedNodeIds.has(node.id) ? " selected" : "");
        card.dataset.nodeId = node.id;
        card.draggable = true;
        card.oncontextmenu = event => showMenu(event, node);
        card.onclick = event => {
          if (clickAfterBoxSelect) {
            clickAfterBoxSelect = false;
            return;
          }
          if (event.ctrlKey || event.metaKey) {
            toggleSelectedNode(node.id);
            return;
          }
          if (selectedNodeIds.size > 0) {
            toggleSelectedNode(node.id);
            return;
          }
          if (node.kind === "folder") openFolder(node.id);
          else location.href = `/api/nodes/${node.id}/download`;
        };

        card.addEventListener("dragstart", () => {
          if (selectedNodeIds.has(node.id) && selectedNodeIds.size > 0) draggedNodeIds = Array.from(selectedNodeIds);
          else draggedNodeIds = [node.id];
        });
        card.addEventListener("dragend", () => {
          draggedNodeIds = [];
          document.querySelectorAll(".drop-target").forEach(x => x.classList.remove("drop-target"));
        });

        if (node.kind === "folder") {
          card.addEventListener("dragover", event => {
            event.preventDefault();
            if (!draggedNodeIds.includes(node.id)) card.classList.add("drop-target");
          });
          card.addEventListener("dragleave", () => card.classList.remove("drop-target"));
          card.addEventListener("drop", async event => {
            event.preventDefault();
            card.classList.remove("drop-target");
            if (draggedNodeIds.length && !draggedNodeIds.includes(node.id)) await moveSelectedNodesTo(node.id);
          });
        }

        card.innerHTML = `
          <button class="more" title="更多操作">⋯</button>
          ${node.shared ? '<div class="badge">Share</div>' : ''}
          <div class="file-icon">${iconFor(node)}</div>
          <div class="card-name">${escapeHtml(node.name)}</div>
          <div class="card-meta">${metaFor(node)}</div>
        `;
        card.querySelector(".more").onclick = event => { event.stopPropagation(); showMenu(event, node); };
        grid.appendChild(card);
      });
    }

    function toggleSelectedNode(nodeId) {
      if (selectedNodeIds.has(nodeId)) selectedNodeIds.delete(nodeId);
      else selectedNodeIds.add(nodeId);
      updateSelectionBar();
    }

    function updateSelectionBar() {
      document.querySelectorAll(".card").forEach(card => {
        if (selectedNodeIds.has(card.dataset.nodeId)) card.classList.add("selected");
        else card.classList.remove("selected");
      });

      const selectionBar = document.getElementById("selectionBar");
      const selectionText = document.getElementById("selectionText");
      if (selectionBar && selectionText) {
        selectionText.textContent = `Selected ${selectedNodeIds.size} 项`;
        selectionBar.style.display = selectedNodeIds.size > 0 ? "flex" : "none";
      }
    }

    function clearSelection() {
      selectedNodeIds.clear();
      updateSelectionBar();
    }

    async function downloadSelectedNodes() {
      const ids = Array.from(selectedNodeIds);
      if (!ids.length) return;
      toast("正在生成 7z 压缩包，请等待Download开始...");
      try {
        const response = await fetch("/api/nodes/download-selected", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ node_ids: ids }),
        });
        if (!response.ok) {
          let message = response.statusText;
          try { message = (await response.json()).message || message; } catch (_) {}
          throw new Error(message);
        }
        const blob = await response.blob();
        saveBlob(blob, "selected.7z");
      } catch (error) { alert(error.message); }
    }

    async function deleteSelectedNodes() {
      const ids = Array.from(selectedNodeIds);
      if (!ids.length) return;
      const ok = confirm(`Delete已选 ${ids.length} 个File或Folder？`);
      if (!ok) return;
      try {
        await api("/api/nodes/delete-selected", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ node_ids: ids }),
        });
        selectedNodeIds.clear();
        updateSelectionBar();
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    async function moveSelectedNodesTo(targetFolderId) {
      const ids = draggedNodeIds.length ? draggedNodeIds : Array.from(selectedNodeIds);
      if (!ids.length) return;
      try {
        await api("/api/nodes/move-selected", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ node_ids: ids, new_parent_id: targetFolderId }),
        });
        selectedNodeIds.clear();
        updateSelectionBar();
        await openFolder(currentFolderId);
        toast("Move complete");
      } catch (error) { alert(error.message); }
    }

    function startBoxSelect(event) {
      if (event.button !== 0) return;
      if (event.target.closest(".card")) return;
      if (event.target.closest(".menu")) return;
      if (event.target.closest("button")) return;
      const grid = document.getElementById("grid");
      if (!grid || !grid.contains(event.target)) return;

      isBoxSelecting = true;
      boxSelectStartX = event.clientX;
      boxSelectStartY = event.clientY;
      clickAfterBoxSelect = false;
      selectedNodeIds.clear();
      updateSelectionBar();

      const box = document.getElementById("dragSelectionBox");
      box.style.left = boxSelectStartX + "px";
      box.style.top = boxSelectStartY + "px";
      box.style.width = "0px";
      box.style.height = "0px";
      box.style.display = "block";
      event.preventDefault();
    }

    function moveBoxSelect(event) {
      if (!isBoxSelecting) return;

      const left = Math.min(boxSelectStartX, event.clientX);
      const top = Math.min(boxSelectStartY, event.clientY);
      const width = Math.abs(event.clientX - boxSelectStartX);
      const height = Math.abs(event.clientY - boxSelectStartY);

      const box = document.getElementById("dragSelectionBox");
      box.style.left = left + "px";
      box.style.top = top + "px";
      box.style.width = width + "px";
      box.style.height = height + "px";

      const selectRectangle = { left, top, right: left + width, bottom: top + height };
      selectedNodeIds.clear();

      document.querySelectorAll("#grid .card").forEach(card => {
        const cardRectangle = card.getBoundingClientRect();
        const intersects = !(
          cardRectangle.right < selectRectangle.left ||
          cardRectangle.left > selectRectangle.right ||
          cardRectangle.bottom < selectRectangle.top ||
          cardRectangle.top > selectRectangle.bottom
        );
        if (intersects) selectedNodeIds.add(card.dataset.nodeId);
      });

      updateSelectionBar();
      event.preventDefault();
    }

    function endBoxSelect(event) {
      if (!isBoxSelecting) return;

      isBoxSelecting = false;
      const box = document.getElementById("dragSelectionBox");
      box.style.display = "none";

      const movedX = Math.abs(event.clientX - boxSelectStartX);
      const movedY = Math.abs(event.clientY - boxSelectStartY);
      if (movedX > 4 || movedY > 4) clickAfterBoxSelect = true;
      event.preventDefault();
    }

    document.getElementById("grid").addEventListener("mousedown", startBoxSelect);
    document.addEventListener("mousemove", moveBoxSelect);
    document.addEventListener("mouseup", endBoxSelect);

    function saveBlob(blob, fileName) {
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = fileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function iconFor(node) {
      if (node.kind === "folder") return "📁";
      if (node.preview_kind === "audio") return "🎵";
      if (node.preview_kind === "video") return "🎬";
      if (node.preview_kind === "text") return "📝";
      return "📄";
    }

    function metaFor(node) {
      if (node.kind === "folder") return "Folder";
      const label = node.preview_kind === "text" ? "Text" : node.preview_kind === "audio" ? "Audio" : node.preview_kind === "video" ? "Video" : "File";
      return `${label} · ${formatSize(node.size)}`;
    }

    function showMenu(event, node) {
      event.preventDefault();
      selectedNode = node || null;
      const menuItems = [];
      const isMultiSelectionMenu = selectedNode && selectedNodeIds.size > 1 && selectedNodeIds.has(selectedNode.id);
      if (isMultiSelectionMenu) {
        menuItems.push(["⬇️", "Download as 7z", downloadSelectedNodes]);
        menuItems.push(["🗑️", "DeleteFile", deleteSelectedNodes]);
      } else if (!selectedNode) {
        menuItems.push(["⬆️", "Upload Files", uploadFromButton, "green"]);
        menuItems.push(["📂", "Upload Folder", uploadFolderFromButton, "green"]);
        menuItems.push(["📁", "New Folder", newFolder]);
        menuItems.push(["🔗", "Share Management", openShareManage, "green"]);
      } else if (selectedNode.kind === "file") {
        menuItems.push(["⬇️", "Download", menuDownload]);
        if (selectedNode.preview_kind !== "none") menuItems.push(["👁️", "在线预览", () => openPreview(selectedNode), "green"]);
        menuItems.push(["🔗", "Create/复制Share链接", menuShare, "green"]);
        if (selectedNode.shared) menuItems.push(["🔒", "Cancel Share", menuCancelShare, "green"]);
        menuItems.push(["✏️", "Rename", menuRename]);
        menuItems.push(["🗑️", "Delete", menuDelete]);
      } else {
        menuItems.push(["📂", "Open", () => openFolder(selectedNode.id), "green"]);
        menuItems.push(["⬇️", "Download as 7z", menuDownload]);
        menuItems.push(["🔗", "Create/复制Share链接", menuShare, "green"]);
        if (selectedNode.shared) menuItems.push(["🔒", "Cancel Share", menuCancelShare, "green"]);
        menuItems.push(["✏️", "Rename", menuRename]);
        menuItems.push(["🗑️", "Delete", menuDelete]);
      }
      menu.innerHTML = "";
      menuItems.forEach((menuItem, itemIndex) => {
        if (itemIndex > 0 && (menuItem[1] === "Rename" || menuItem[1] === "Delete")) {
          const dividerElement = document.createElement("div");
          dividerElement.className = "menu-divider";
          menu.appendChild(dividerElement);
        }
        const menuItemElement = document.createElement("div");
        menuItemElement.className = "menu-row" + (menuItem[3] ? " " + menuItem[3] : "");
        menuItemElement.innerHTML = `<span>${menuItem[0]}</span><span>${menuItem[1]}</span>`;
        menuItemElement.onclick = () => { menu.style.display = "none"; menuItem[2](); };
        menu.appendChild(menuItemElement);
      });
      menu.style.display = "block";
      menu.style.left = Math.min(event.clientX, window.innerWidth - 248) + "px";
      menu.style.top = Math.min(event.clientY, window.innerHeight - 340) + "px";
    }

    document.addEventListener("click", () => { menu.style.display = "none"; });
    document.getElementById("mainArea").addEventListener("contextmenu", event => {
      if (event.target.closest(".card")) return;
      showMenu(event, null);
    });

    function uploadFromButton() { document.getElementById("fileInput").click(); }
    function uploadFolderFromButton() { document.getElementById("folderInput").click(); }
    function openShareManage() { window.open("/shares", "_blank"); }

    async function newFolder() {
      const name = prompt("Folder名称：");
      if (!name) return;
      try {
        await api("/api/folders", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ parent_id: currentFolderId, name }),
        });
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    function menuDownload() {
      if (!selectedNode) return;
      if (selectedNode.kind === "folder") toast("正在压缩Folder，请等待Download开始...");
      location.href = `/api/nodes/${selectedNode.id}/download`;
    }

    function openPreview(node) {
      if (!node || node.kind !== "file" || node.preview_kind === "none") return;
      window.open(`/view/${node.id}`, "_blank");
    }

    async function menuShare() {
      if (!selectedNode) return;
      try {
        const responseData = await api(`/api/nodes/${selectedNode.id}/share`, { method: "POST" });
        const url = location.origin + responseData.url;
        try { await navigator.clipboard.writeText(url); toast("Share链接已复制"); }
        catch (_) { prompt("Share链接：", url); }
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    async function menuCancelShare() {
      if (!selectedNode) return;
      try {
        await api(`/api/nodes/${selectedNode.id}/share`, { method: "DELETE" });
        toast("已Cancel Share");
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    async function menuRename() {
      if (!selectedNode) return;
      const name = prompt("新名称：", selectedNode.name);
      if (!name || name === selectedNode.name) return;
      try {
        await api(`/api/nodes/${selectedNode.id}/rename`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name }),
        });
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    async function menuDelete() {
      if (!selectedNode) return;
      const ok = confirm(selectedNode.kind === "folder" ? `DeleteFolder「${selectedNode.name}」及其所有内容？` : `DeleteFile「${selectedNode.name}」？`);
      if (!ok) return;
      try {
        await api(`/api/nodes/${selectedNode.id}`, { method: "DELETE" });
        await openFolder(currentFolderId);
      } catch (error) { alert(error.message); }
    }

    async function uploadOneFile(file, parentId, displayName) {
      const form = new FormData();
      form.append("parent_id", parentId);
      form.append("name", displayName || file.name);
      form.append("file", file, displayName || file.name);
      const res = await fetch("/api/files", { method: "POST", body: form });
      if (!res.ok) {
        let msg = res.statusText;
        try { msg = (await res.json()).message || msg; } catch (_) {}
        throw new Error(msg);
      }
    }

    async function uploadSelectedFiles(files) {
      if (!files || !files.length) return;
      for (const file of files) {
        try {
          toast("Uploading: " + file.name);
          await uploadOneFile(file, currentFolderId, file.name);
        } catch (error) { alert(`上传 ${file.name} 失败：${error.message}`); }
      }
      document.getElementById("fileInput").value = "";
      await openFolder(currentFolderId);
      toast("Upload complete");
    }

    async function uploadFolderSelected(files) {
      if (!files || !files.length) return;
      const folderCache = new Map();

      async function ensureFolder(parentId, name) {
        const key = parentId + "/" + name;
        if (folderCache.has(key)) return folderCache.get(key);

        const children = await api(`/api/nodes/${parentId}/children`);
        const existed = children.items.find(x => x.kind === "folder" && x.name === name);
        if (existed) {
          folderCache.set(key, existed.id);
          return existed.id;
        }

        const created = await api("/api/folders", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ parent_id: parentId, name }),
        });
        folderCache.set(key, created.id);
        return created.id;
      }

      for (const file of files) {
        const relativePath = file.webkitRelativePath || file.name;
        const parts = relativePath.split("/").filter(Boolean);
        const fileName = parts.pop() || file.name;
        let targetFolderId = currentFolderId;

        try {
          for (const folderName of parts) {
            targetFolderId = await ensureFolder(targetFolderId, folderName);
          }
          toast("Uploading: " + relativePath);
          await uploadOneFile(file, targetFolderId, fileName);
        } catch (error) { alert(`上传 ${relativePath} 失败：${error.message}`); }
      }

      document.getElementById("folderInput").value = "";
      await openFolder(currentFolderId);
      toast("Folder upload complete");
    }

    async function moveNode(nodeId, targetFolderId) {
      try {
        await api(`/api/nodes/${nodeId}/move`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ new_parent_id: targetFolderId }),
        });
        await openFolder(currentFolderId);
        toast("Move complete");
      } catch (error) { alert(error.message); }
    }

    function formatSize(sizeBytes) {
      if (sizeBytes < 1024) return sizeBytes + " B";
      if (sizeBytes < 1048576) return (sizeBytes / 1024).toFixed(1) + " KB";
      if (sizeBytes < 1073741824) return (sizeBytes / 1048576).toFixed(1) + " MB";
      return (sizeBytes / 1073741824).toFixed(1) + " GB";
    }

    function escapeHtml(s) {
      return String(s).replace(/[&<>\"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[c]));
    }

    boot();
  </script>
</body>
</html>
"###;


const VIEW_HTML: &str = r###"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>Online Preview</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --orange: #ff5a1f;
      --orange-dark: #d94716;
      --orange-soft: #fff0e8;
      --green: #e9f6e9;
      --green-strong: rgb(0, 60, 0);
      --green-line: #cce8cc;
      --text: #2f201b;
      --muted: #80635b;
    }
    * { box-sizing: border-box; }
    html, body {
      margin: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background: #fff7f1;
    }
    body { display: flex; flex-direction: column; }
    .bar {
      height: 40px;
      flex: 0 0 40px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 10px;
      background: linear-gradient(90deg, var(--orange), #ff7b4c);
      color: white;
    }
    .title { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-weight: 900; }
    .meta { font-size: 12px; color: rgba(255,255,255,.82); white-space: nowrap; }
    button {
      border: 0;
      border-radius: 10px;
      padding: 7px 12px;
      cursor: pointer;
      font-weight: 850;
      background: var(--green);
      color: var(--green-strong);
      border: 1px solid var(--green-line);
    }
    button.primary { background: var(--orange-soft); color: var(--orange-dark); border-color: transparent; }
    textarea {
      flex: 1;
      width: 100%;
      min-height: 0;
      resize: none;
      border: 0;
      outline: none;
      padding: 16px;
      font: 14px/1.6 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre;
      tab-size: 4;
      overflow: auto;
      color: #111;
      background: white;
    }
    .viewer { flex: 1; min-height: 0; display: grid; place-items: center; padding: 0; background: #111; }
    video { max-width: 100%; max-height: 100%; }
    audio { width: min(760px, 92vw); }
    .message { flex: 1; display: grid; place-items: center; padding: 18px; color: var(--muted); }
  </style>
</head>
<body>
  <div class="bar">
    <div id="title" class="title">Online Preview</div>
    <div id="meta" class="meta"></div>
    <button id="downloadBtn">Download</button>
    <button id="saveBtn" class="primary" style="display:none;">Save</button>
  </div>
  <div id="root" class="message">Opening...</div>

  <script>
    const nodeId = location.pathname.split("/").pop();
    let node = null;
    async function api(path, options = {}) {
      const res = await fetch(path, options);
      if (!res.ok) {
        let msg = res.statusText;
        try { msg = (await res.json()).message || msg; } catch (_) {}
        throw new Error(msg);
      }
      if (res.status === 204) return null;
      return res.json();
    }
    async function init() {
      try {
        node = await api(`/api/nodes/${nodeId}`);
        document.title = node.name;
        document.getElementById("title").textContent = node.name;
        document.getElementById("downloadBtn").onclick = () => location.href = `/api/nodes/${node.id}/download`;
        if (node.preview_kind === "text") {
          const responseData = await api(`/api/nodes/${node.id}/text`);
          document.getElementById("meta").textContent = `编码：${responseData.encoding}`;
          const textarea = document.createElement("textarea");
          textarea.id = "editor";
          textarea.spellcheck = false;
          textarea.value = responseData.content;
          document.getElementById("root").replaceWith(textarea);
          document.getElementById("saveBtn").style.display = "inline-block";
          document.getElementById("saveBtn").onclick = saveText;
          textarea.focus();
        } else if (node.preview_kind === "audio") {
          const root = document.getElementById("root");
          root.className = "viewer";
          root.innerHTML = `<audio controls autoplay src="/api/nodes/${node.id}/preview"></audio>`;
        } else if (node.preview_kind === "video") {
          const root = document.getElementById("root");
          root.className = "viewer";
          root.innerHTML = `<video controls autoplay src="/api/nodes/${node.id}/preview"></video>`;
        } else {
          document.getElementById("root").textContent = "该FileType暂不支持Online Preview。";
        }
      } catch (error) { document.getElementById("root").textContent = error.message; }
    }
    async function saveText() {
      const content = document.getElementById("editor").value;
      try {
        const responseData = await api(`/api/nodes/${node.id}/text`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ content }),
        });
        document.getElementById("meta").textContent = `已Save · 编码：${responseData.encoding}`;
      } catch (error) { alert(error.message); }
    }
    init();
  </script>
</body>
</html>
"###;

const SHARES_MANAGE_HTML: &str = r###"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>Share Management</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --orange: #ff5a1f;
      --orange-dark: #d94716;
      --orange-soft: #fff0e8;
      --orange-line: #ffd5c1;
      --green: #e9f6e9;
      --green-strong: rgb(0, 60, 0);
      --green-line: #cce8cc;
      --text: #2f201b;
      --muted: #80635b;
      --panel: #fff;
      --shadow: 0 18px 46px rgba(126, 48, 14, .10);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: radial-gradient(circle at 12% 0%, rgba(255,90,31,.14), transparent 28rem), linear-gradient(180deg, #fff9f4, #fff2ea);
      color: var(--text);
    }
    .topbar {
      height: 40px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      background: linear-gradient(90deg, var(--orange), #ff7b4c);
      color: white;
      box-shadow: 0 2px 14px rgba(223,63,13,.20);
    }
    .brand { font-weight: 900; }
    button {
      border: 0;
      border-radius: 12px;
      padding: 9px 13px;
      cursor: pointer;
      font-weight: 850;
      background: var(--orange);
      color: white;
    }
    button:hover { background: var(--orange-dark); }
    button.green { background: var(--green); color: var(--green-strong); border: 1px solid var(--green-line); }
    main { width: 100%; margin: 0; padding: 10px; }
    .panel { background: var(--panel); border: 1px solid var(--orange-line); border-radius: 16px; overflow: hidden; box-shadow: var(--shadow); }
    .head { min-height: 70px; padding: 16px 18px; border-bottom: 1px solid var(--orange-line); background: rgba(255,255,255,.72); }
    h1 { margin: 0; font-size: 22px; }
    .sub { margin-top: 5px; color: var(--muted); font-size: 13px; }
    .list { padding: 10px; }
    .item { display: flex; align-items: center; justify-content: space-between; gap: 14px; min-height: 58px; padding: 10px 12px; border-radius: 12px; border: 1px solid transparent; }
    .item:hover { border-color: var(--orange-line); background: #fffaf7; }
    .name-line { display: flex; align-items: baseline; gap: 8px; min-width: 0; flex-wrap: wrap; }
    .name { font-weight: 850; word-break: break-word; }
    .meta { color: var(--muted); font-size: 12px; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
    .empty { min-height: 280px; display: grid; place-items: center; text-align: center; color: var(--muted); font-weight: 700; }
    @media (max-width: 720px) { .item { align-items: flex-start; flex-direction: column; } .actions { justify-content: flex-start; } }
  </style>
</head>
<body>
  <nav class="topbar">
    <div class="brand">RustDrive · Share Management</div>
    <button class="green" onclick="location.href='/'">Back to Drive</button>
  </nav>
  <main>
    <section class="panel">
      <div class="head">
        <h1>Share Management</h1>
        <div id="summary" class="sub">Loading...</div>
      </div>
      <div id="list" class="list"></div>
    </section>
  </main>
  <script>
    async function api(path, options = {}) {
      const res = await fetch(path, options);
      if (!res.ok) {
        let msg = res.statusText;
        try { msg = (await res.json()).message || msg; } catch (_) {}
        throw new Error(msg);
      }
      if (res.status === 204) return null;
      return res.json();
    }
    async function loadShares() {
      const list = document.getElementById('list');
      try {
        const responseData = await api('/api/shares');
        document.getElementById('summary').textContent = `${responseData.items.length} 个Share`;
        list.innerHTML = '';
        if (!responseData.items.length) {
          list.innerHTML = '<div class="empty"><div>No shares yet<br><span style="font-weight:500;font-size:13px;">回到Drive右键File或Folder即可CreateShare。</span></div></div>';
          return;
        }
        responseData.items.forEach(item => {
          const url = location.origin + item.url;
          const div = document.createElement('div');
          div.className = 'item';
          div.innerHTML = `
            <div>
              <div class="name-line">
                <span class="name">${iconFor(item.node)} ${escapeHtml(item.node.name)}</span>
                <span class="meta">${item.node.kind === 'folder' ? 'Folder' : 'File'} · ${formatDate(item.created_at)}</span>
              </div>
            </div>
            <div class="actions">
              <button class="green" data-act="open">Open</button>
              <button class="green" data-act="copy">Copy Link</button>
              <button data-act="cancel">Cancel Share</button>
            </div>`;
          div.querySelector('[data-act="open"]').onclick = () => window.open(item.url, '_blank');
          div.querySelector('[data-act="copy"]').onclick = async () => {
            try { await navigator.clipboard.writeText(url); alert('已复制'); }
            catch (_) { prompt('Share链接：', url); }
          };
          div.querySelector('[data-act="cancel"]').onclick = async () => {
            if (!confirm('OKCancel这个Share？')) return;
            await api(`/api/shares/${item.token}`, { method: 'DELETE' });
            await loadShares();
          };
          list.appendChild(div);
        });
      } catch (error) { list.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`; }
    }
    function formatDate(value) {
      if (!value) return "";
      return String(value).slice(0, 10);
    }
    function iconFor(node) {
      if (node.kind === 'folder') return '📁';
      if (node.preview_kind === 'audio') return '🎵';
      if (node.preview_kind === 'video') return '🎬';
      if (node.preview_kind === 'text') return '📝';
      return '📄';
    }
    function escapeHtml(s) { return String(s).replace(/[&<>\"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }
    loadShares();
  </script>
</body>
</html>
"###;

const SHARE_HTML: &str = r###"
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>Shared View</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --orange: #ff5a1f;
      --orange-dark: #d94716;
      --orange-soft: #fff0e8;
      --orange-line: #ffd5c1;
      --green: #e9f6e9;
      --green-strong: rgb(0, 60, 0);
      --green-line: #cce8cc;
      --bg: #fff7f1;
      --text: #2f201b;
      --muted: #80635b;
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; }
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #fff9f4 0%, #fff2ea 100%);
      color: var(--text);
    }
    .topbar {
      height: 40px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 12px;
      background: linear-gradient(90deg, var(--orange), #ff7b4c);
      color: white;
      position: sticky;
      top: 0;
      z-index: 20;
    }
    .brand { font-weight: 900; white-space: nowrap; font-size: 13px; }
    .crumbs {
      flex: 1;
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 4px;
      overflow-x: auto;
      scrollbar-width: none;
    }
    .crumbs::-webkit-scrollbar { display: none; }
    .crumb {
      height: 21px;
      display: inline-flex;
      align-items: center;
      padding: 0 8px;
      border-radius: 999px;
      background: rgba(255,255,255,.18);
      color: white;
      font-size: 12px;
      cursor: pointer;
      white-space: nowrap;
    }
    .crumb:hover { background: rgba(255,255,255,.34); }
    .crumb-sep { color: rgba(255,255,255,.68); }
    button {
      border: 0;
      border-radius: 9px;
      padding: 6px 10px;
      cursor: pointer;
      font-weight: 850;
      font-size: 12px;
      background: var(--orange-soft);
      color: var(--orange-dark);
    }
    button.green {
      background: var(--green);
      color: var(--green-strong);
      border: 1px solid var(--green-line);
    }
    .main { width: 100%; margin: 0; padding: 12px; }
    .grid {
      min-height: calc(100vh - 62px);
      display: grid;
      grid-template-columns: repeat(auto-fill, 132px);
      align-content: start;
      justify-content: start;
      gap: 12px;
    }
    .card {
      position: relative;
      width: 132px;
      height: 132px;
      padding: 10px;
      border-radius: 14px;
      border: 1px solid #ffe0d1;
      background: white;
      box-shadow: 0 8px 20px rgba(126,48,14,.045);
      cursor: pointer;
      user-select: none;
    }
    .card:hover { border-color: #ff9b77; background: #fffaf7; }
    .card.selected { border-color: var(--orange); background: var(--orange-soft); outline: 2px solid rgba(255,90,31,.18); }
    .drag-selection-box { position: fixed; z-index: 80; display: none; pointer-events: none; border: 1px solid var(--orange); background: rgba(255,90,31,.10); }
    .file-icon {
      width: 32px;
      height: 32px;
      display: grid;
      place-items: center;
      border-radius: 4px;
      background: var(--orange-soft);
      font-size: 17px;
      margin-bottom: 8px;
    }
    .card.folder .file-icon { background: var(--green); }
    .card-name {
      font-weight: 850;
      font-size: 13px;
      line-height: 1.25;
      word-break: break-word;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }
    .card-meta { margin-top: 5px; color: var(--muted); font-size: 11px; }
    .viewer {
      min-height: calc(100vh - 62px);
      background: #fffaf7;
      border: 1px solid var(--orange-line);
      border-radius: 14px;
      overflow: hidden;
    }
    .viewer-head {
      min-height: 42px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 8px 10px;
      border-bottom: 1px solid var(--orange-line);
      background: white;
    }
    .viewer-title { font-weight: 900; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .viewer-body { padding: 12px; }
    textarea {
      width: 100%;
      height: calc(100vh - 128px);
      resize: none;
      border: 0;
      outline: none;
      padding: 14px;
      font: 14px/1.6 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      white-space: pre;
      tab-size: 4;
      background: white;
      color: #111;
    }
    video, audio { width: 100%; background: #111; }
    video { max-height: calc(100vh - 128px); }
    .empty {
      min-height: 260px;
      display: grid;
      place-items: center;
      color: var(--muted);
      text-align: center;
      font-weight: 700;
    }
    .selection-bar {
      position: fixed;
      left: 50%;
      bottom: 18px;
      transform: translateX(-50%);
      display: none;
      align-items: center;
      gap: 10px;
      z-index: 90;
      padding: 10px 12px;
      border-radius: 18px;
      background: white;
      border: 1px solid var(--orange-line);
      box-shadow: 0 22px 54px rgba(61,24,9,.18);
      font-weight: 850;
    }
    .menu {
      position: fixed;
      min-width: 190px;
      display: none;
      z-index: 100;
      background: white;
      border: 1px solid var(--orange-line);
      border-radius: 16px;
      padding: 8px;
      box-shadow: 0 22px 54px rgba(61,24,9,.20);
    }
    .menu-row {
      min-height: 38px;
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 0 10px;
      border-radius: 11px;
      font-weight: 800;
      cursor: pointer;
    }
    .menu-row:hover { background: var(--orange-soft); color: var(--orange-dark); }
  </style>
</head>
<body>
  <nav class="topbar">
    <div class="brand">RustDrive · Read-only Share</div>
    <div id="crumbs" class="crumbs"></div>
    <button class="green" onclick="downloadCurrent()">Download</button>
  </nav>
  <main class="main">
    <div id="content" class="grid"></div>
  </main>
  <div id="shareMenu" class="menu"></div>
  <div id="shareSelectionBar" class="selection-bar">
    <span id="shareSelectionText">Selected 0 项</span>
    <button class="green" onclick="downloadSelectedSharedNodes()">Download as 7z</button>
    <button onclick="clearShareSelection()">Clear Selection</button>
  </div>
  <div id="shareDragSelectionBox" class="drag-selection-box"></div>

  <script>
    const token = location.pathname.split("/").pop();
    let share = null;
    let rootNode = null;
    let currentNode = null;
    let selectedShareNodeIds = new Set();
    let isShareBoxSelecting = false;
    let shareBoxSelectStartX = 0;
    let shareBoxSelectStartY = 0;
    let ignoreShareClickAfterBoxSelect = false;
    const shareMenu = document.getElementById("shareMenu");

    async function api(path) {
      const res = await fetch(path);
      if (!res.ok) {
        let msg = res.statusText;
        try { msg = (await res.json()).message || msg; } catch (_) {}
        throw new Error(msg);
      }
      return res.json();
    }

    async function load() {
      try {
        share = await api(`/api/public/shares/${token}`);
        rootNode = share.node;
        if (rootNode.kind === "folder") await openFolder(rootNode.id);
        else await openFile(rootNode);
      } catch (error) {
        document.getElementById("crumbs").innerHTML = "";
        document.getElementById("content").className = "empty";
        document.getElementById("content").textContent = error.message;
      }
    }

    async function openFolder(folderId) {
      currentNode = { id: folderId, kind: "folder" };
      selectedShareNodeIds.clear();
      updateShareSelectionBar();
      await renderCrumbs(folderId);
      const responseData = await api(`/api/public/shares/${token}/nodes/${folderId}/children`);
      renderGrid(responseData.items);
    }

    async function openFile(node) {
      currentNode = node;
      selectedShareNodeIds.clear();
      updateShareSelectionBar();
      await renderCrumbs(node.id);
      const content = document.getElementById("content");
      content.className = "viewer";
      content.innerHTML = `
        <div class="viewer-head">
          <div class="viewer-title">${iconFor(node)} ${escapeHtml(node.name)}</div>
        </div>
        <div id="viewerBody" class="viewer-body"></div>`;
      const body = document.getElementById("viewerBody");

      if (node.preview_kind === "text") {
        const responseData = await api(`/api/public/shares/${token}/nodes/${node.id}/text`);
        const textarea = document.createElement("textarea");
        textarea.value = responseData.content;
        textarea.readOnly = true;
        textarea.spellcheck = false;
        body.replaceWith(textarea);
      } else if (node.preview_kind === "audio") {
        body.innerHTML = `<audio controls autoplay src="/api/public/shares/${token}/nodes/${node.id}/preview"></audio>`;
      } else if (node.preview_kind === "video") {
        body.innerHTML = `<video controls autoplay src="/api/public/shares/${token}/nodes/${node.id}/preview"></video>`;
      } else {
        body.innerHTML = '<div class="empty">该FileType暂不支持在线预览，可以DownloadView。</div>';
      }
    }

    async function renderCrumbs(nodeId) {
      const responseData = await api(`/api/public/shares/${token}/nodes/${nodeId}/breadcrumbs`);
      const element = document.getElementById("crumbs");
      element.innerHTML = "";
      responseData.items.forEach((item, itemIndex) => {
        const span = document.createElement("span");
        span.className = "crumb";
        span.textContent = item.name;
        if (!(currentNode && currentNode.kind === "file" && itemIndex === responseData.items.length - 1)) {
          span.onclick = () => openFolder(item.id);
        }
        element.appendChild(span);
        if (itemIndex < responseData.items.length - 1) {
          const separatorElement = document.createElement("span");
          separatorElement.className = "crumb-sep";
          separatorElement.textContent = "›";
          element.appendChild(separatorElement);
        }
      });
    }

    function renderGrid(items) {
      const content = document.getElementById("content");
      content.className = "grid";
      content.innerHTML = "";
      if (!items.length) {
        content.innerHTML = '<div class="empty">这个Folder是空的</div>';
        return;
      }
      items.forEach(node => {
        const card = document.createElement("div");
        card.className = `card ${node.kind}` + (selectedShareNodeIds.has(node.id) ? " selected" : "");
        card.dataset.nodeId = node.id;
        card.oncontextmenu = event => showShareMenu(event, node);
        card.onclick = event => {
          if (ignoreShareClickAfterBoxSelect) {
            ignoreShareClickAfterBoxSelect = false;
            return;
          }
          if (event.ctrlKey || event.metaKey) {
            toggleSharedNode(node.id);
            return;
          }
          if (selectedShareNodeIds.size > 1 && selectedShareNodeIds.has(node.id)) {
            downloadSelectedSharedNodes();
            return;
          }
          if (selectedShareNodeIds.size > 0) {
            toggleSharedNode(node.id);
            return;
          }
          downloadSharedNode(node);
        };
        card.innerHTML = `
          <div class="file-icon">${iconFor(node)}</div>
          <div class="card-name">${escapeHtml(node.name)}</div>
          <div class="card-meta">${metaFor(node)}</div>`;
        content.appendChild(card);
      });
    }

    function downloadCurrent() {
      if (!currentNode) return;
      location.href = `/api/public/shares/${token}/nodes/${currentNode.id}/download`;
    }

    function downloadSharedNode(node) {
      location.href = `/api/public/shares/${token}/nodes/${node.id}/download`;
    }

    function previewSharedNode(node) {
      node.kind === "folder" ? openFolder(node.id) : openFile(node);
    }

    function previewMenuText(node) {
      if (node.kind === "folder") return "Open/View";
      if (node.preview_kind === "audio") return "在线播放";
      if (node.preview_kind === "video") return "在线播放";
      if (node.preview_kind === "text") return "Online Preview";
      return "在线预览";
    }

    function toggleSharedNode(nodeId) {
      if (selectedShareNodeIds.has(nodeId)) selectedShareNodeIds.delete(nodeId);
      else selectedShareNodeIds.add(nodeId);
      updateShareSelectionBar();
    }

    function updateShareSelectionBar() {
      document.querySelectorAll(".card").forEach(card => {
        if (selectedShareNodeIds.has(card.dataset.nodeId)) card.classList.add("selected");
        else card.classList.remove("selected");
      });
      const selectionBar = document.getElementById("shareSelectionBar");
      const selectionText = document.getElementById("shareSelectionText");
      if (!selectionBar || !selectionText) return;
      const count = selectedShareNodeIds.size;
      selectionText.textContent = `Selected ${count} 项`;
      selectionBar.style.display = count > 0 ? "flex" : "none";
    }

    function clearShareSelection() {
      selectedShareNodeIds.clear();
      updateShareSelectionBar();
    }

    function showShareMenu(event, node) {
      event.preventDefault();
      event.stopPropagation();
      shareMenu.innerHTML = "";
      const menuItemElement = document.createElement("div");
      menuItemElement.className = "menu-row";
      if (selectedShareNodeIds.size > 1 && selectedShareNodeIds.has(node.id)) {
        menuItemElement.innerHTML = `<span>⬇️</span><span>Download as 7z</span>`;
        menuItemElement.onclick = () => {
          shareMenu.style.display = "none";
          downloadSelectedSharedNodes();
        };
      } else {
        menuItemElement.innerHTML = `<span>👁️</span><span>${previewMenuText(node)}</span>`;
        menuItemElement.onclick = () => {
          shareMenu.style.display = "none";
          previewSharedNode(node);
        };
      }
      shareMenu.appendChild(menuItemElement);
      shareMenu.style.display = "block";
      shareMenu.style.left = Math.min(event.clientX, window.innerWidth - 210) + "px";
      shareMenu.style.top = Math.min(event.clientY, window.innerHeight - 80) + "px";
    }

    document.addEventListener("click", () => {
      if (shareMenu) shareMenu.style.display = "none";
    });

    function startShareBoxSelect(event) {
      if (event.button !== 0) return;
      if (event.target.closest(".card")) return;
      if (event.target.closest(".menu")) return;
      if (event.target.closest("button")) return;
      const content = document.getElementById("content");
      if (!content || !content.classList.contains("grid")) return;
      if (!content.contains(event.target)) return;

      isShareBoxSelecting = true;
      shareBoxSelectStartX = event.clientX;
      shareBoxSelectStartY = event.clientY;
      ignoreShareClickAfterBoxSelect = false;
      selectedShareNodeIds.clear();
      updateShareSelectionBar();

      const box = document.getElementById("shareDragSelectionBox");
      box.style.left = shareBoxSelectStartX + "px";
      box.style.top = shareBoxSelectStartY + "px";
      box.style.width = "0px";
      box.style.height = "0px";
      box.style.display = "block";
      event.preventDefault();
    }

    function moveShareBoxSelect(event) {
      if (!isShareBoxSelecting) return;

      const left = Math.min(shareBoxSelectStartX, event.clientX);
      const top = Math.min(shareBoxSelectStartY, event.clientY);
      const width = Math.abs(event.clientX - shareBoxSelectStartX);
      const height = Math.abs(event.clientY - shareBoxSelectStartY);

      const box = document.getElementById("shareDragSelectionBox");
      box.style.left = left + "px";
      box.style.top = top + "px";
      box.style.width = width + "px";
      box.style.height = height + "px";

      const selectRectangle = { left, top, right: left + width, bottom: top + height };
      selectedShareNodeIds.clear();

      document.querySelectorAll("#content.grid .card").forEach(card => {
        const cardRectangle = card.getBoundingClientRect();
        const intersects = !(
          cardRectangle.right < selectRectangle.left ||
          cardRectangle.left > selectRectangle.right ||
          cardRectangle.bottom < selectRectangle.top ||
          cardRectangle.top > selectRectangle.bottom
        );
        if (intersects) selectedShareNodeIds.add(card.dataset.nodeId);
      });

      updateShareSelectionBar();
      event.preventDefault();
    }

    function endShareBoxSelect(event) {
      if (!isShareBoxSelecting) return;

      isShareBoxSelecting = false;
      const box = document.getElementById("shareDragSelectionBox");
      box.style.display = "none";

      const movedX = Math.abs(event.clientX - shareBoxSelectStartX);
      const movedY = Math.abs(event.clientY - shareBoxSelectStartY);
      if (movedX > 4 || movedY > 4) ignoreShareClickAfterBoxSelect = true;
      event.preventDefault();
    }

    document.getElementById("content").addEventListener("mousedown", startShareBoxSelect);
    document.addEventListener("mousemove", moveShareBoxSelect);
    document.addEventListener("mouseup", endShareBoxSelect);

    async function downloadSelectedSharedNodes() {
      const ids = Array.from(selectedShareNodeIds);
      if (!ids.length) return;
      const response = await fetch(`/api/public/shares/${token}/download-selected`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ node_ids: ids }),
      });
      if (!response.ok) {
        let message = response.statusText;
        try { message = (await response.json()).message || message; } catch (_) {}
        alert(message);
        return;
      }
      const blob = await response.blob();
      saveBlob(blob, "shared-selected.7z");
    }

    function saveBlob(blob, fileName) {
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = fileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }

    function metaFor(node) {
      if (node.kind === "folder") return "Folder";
      const label = node.preview_kind === "text" ? "Text" : node.preview_kind === "audio" ? "Audio" : node.preview_kind === "video" ? "Video" : "File";
      return `${label} · ${formatSize(node.size)}`;
    }
    function iconFor(node) {
      if (node.kind === "folder") return "📁";
      if (node.preview_kind === "audio") return "🎵";
      if (node.preview_kind === "video") return "🎬";
      if (node.preview_kind === "text") return "📝";
      return "📄";
    }
    function formatSize(sizeBytes) {
      if (sizeBytes < 1024) return sizeBytes + " B";
      if (sizeBytes < 1048576) return (sizeBytes / 1024).toFixed(1) + " KB";
      if (sizeBytes < 1073741824) return (sizeBytes / 1048576).toFixed(1) + " MB";
      return (sizeBytes / 1073741824).toFixed(1) + " GB";
    }
    function escapeHtml(s) {
      return String(s).replace(/[&<>\"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[c]));
    }
    load();
  </script>
</body>
</html>
"###;

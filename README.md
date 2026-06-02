# RustDrive ☁️

RustDrive is a lightweight self-hosted cloud drive written in Rust. It is built with Axum, Tokio, SQLx, and SQLite, and provides a simple private file storage system with user accounts, folder management, file sharing, browser preview, and compressed downloads.

The project is designed for people who want a small, practical cloud drive without running a full enterprise storage stack. It stores metadata in SQLite and stores uploaded file objects on the local filesystem, so it does not require MySQL, PostgreSQL, Redis, S3, or any external object storage service.

## Features

### File and folder management

RustDrive supports common cloud-drive operations, including file upload, file download, file listing, folder creation, renaming, moving, and deletion. Files and folders are organized as a multi-level directory tree, making it suitable for personal documents, project files, media files, temporary archives, and LAN file sharing.

The file list keeps folders before files and sorts entries by name. Each file record stores metadata such as name, size, MIME type, storage key, creation time, and update time. The app also includes breadcrumb navigation so users can move through nested folders more easily.

### Upload handling

The upload flow writes incoming files in chunks instead of loading the whole file into memory at once. During upload, RustDrive records the final file size, detects the MIME type, validates the file name, and stores the file as an object under the local object directory.

RustDrive also handles same-name uploads carefully. If a file with the same name already exists, the app compares the uploaded content and can reuse the existing file record when the content is identical. If the name is the same but the content is different, RustDrive resolves the conflict safely instead of overwriting the existing file.

### Hash verification and duplicate detection

RustDrive uses SHA-256 hashing to compare uploaded file content. This allows the app to detect duplicate content during same-name upload handling and avoid unnecessary duplicate records when the uploaded file is already present.

This is especially useful when uploading folders or uploading many files from a browser, where repeated files and name conflicts are common.

### 7z compressed downloads

RustDrive supports downloading selected files and folders as a single `.7z` archive. Users can select multiple files, multiple folders, or a mixed set of files and folders, then download everything as one compressed archive.

The compression feature is implemented with `sevenz-rust` and LZMA-based 7z writing. It is useful for downloading many small files at once, exporting a full folder tree, or sharing a clean packaged archive with others.

### Public sharing

RustDrive supports public sharing for both files and folders. A logged-in user can create or cancel a share link for a file or folder, and shared content can be accessed through a read-only public page.

For shared files, visitors can download the file or preview it directly in the browser when the file type is supported. For shared folders, visitors can browse child folders, open files, download individual items, and download selected shared content as a `.7z` archive.

### Online preview

RustDrive includes browser-based preview for common file types. Text files can be viewed online, images and PDFs can be opened in the browser, videos can be played online, and audio files can be streamed directly from the share page or the private file view.

The app also includes text encoding detection for reading text files more reliably. Text files have size limits for viewing and editing to avoid accidentally opening very large files in the browser.

### Text editing

In addition to viewing text files, RustDrive supports editing and saving text files from the browser for supported text content. This makes it convenient for quick edits to notes, configuration files, logs, scripts, and small documents without downloading and re-uploading the file.

### Batch operations

RustDrive supports multi-selection in the file manager. Users can select multiple files or folders and perform batch operations such as deletion, moving, and compressed download.

Batch download also works for shared folders, allowing public visitors to package selected shared items into one `.7z` archive.

### Local storage design

RustDrive uses a simple local storage layout:

```text
drive_data/
├── drive.sqlite3
├── objects/
└── tmp/
```

`drive.sqlite3` stores users, sessions, file nodes, folder nodes, and share records. `objects/` stores uploaded file objects. `tmp/` stores temporary files such as generated 7z archives.

For backup or migration, copy the entire `drive_data` directory. The database and object directory should be kept together.

## Main capabilities

### User system

- User registration
- User login
- User logout
- Cookie-based sessions
- Automatic root folder creation for each user

### File manager

- Upload files
- Download files
- Create folders
- Browse folders
- Rename files and folders
- Move files and folders
- Delete files and folders
- Multi-level directory tree
- Breadcrumb navigation
- Same-name conflict handling
- MIME type detection
- File size tracking
- SHA-256 content comparison

### Batch tools

- Multi-select files
- Multi-select folders
- Batch delete
- Batch move
- Batch download
- Mixed file and folder archive export
- 7z compressed archive generation

### Preview and editing

- Text file preview
- Text file editing
- Text file saving
- Text encoding detection
- Image preview
- PDF preview
- Video playback
- Audio playback
- Preview support in both private and public share pages

### Sharing

- Share files
- Share folders
- Cancel share links
- Manage share list
- Public read-only share pages
- Browse shared folders
- Browse shared subfolders
- Download shared files
- Preview shared files
- View shared text files
- Play shared video files
- Play shared audio files
- Download selected shared items as 7z

### Storage

- SQLite metadata storage
- Local filesystem object storage
- Local temporary archive storage
- No external database required
- No Redis required
- No external object storage required

## Tech stack

RustDrive is built with:

- Rust 2021
- Axum
- Tokio
- SQLx
- SQLite
- Serde
- axum-extra
- UUID
- SHA-256 via `sha2`
- 7z compression via `sevenz-rust`
- Text encoding detection via `chardetng`
- Chrono and time utilities

## Project structure

```text
RustDrive/
├── Cargo.toml
├── LICENSE
├── README.md
└── src/
    └── main.rs
```

## Quick start

Clone the repository:

```bash
git clone https://github.com/wangyifan349/RustDrive.git
cd RustDrive
```

Build the project:

```bash
cargo build --release
```

Run the project:

```bash
cargo run --release
```

After startup, open the local address printed in the terminal. Register a user account first, then you can start uploading, managing, previewing, sharing, and downloading files.

## Backup

RustDrive keeps its runtime data under `drive_data`. For a full backup, copy the entire directory:

```text
drive_data/
```

Do not back up only the database or only the object directory. The SQLite database and stored objects must stay consistent with each other.

## Use cases

RustDrive is suitable for:

- Personal self-hosted cloud storage
- Home NAS file management
- LAN file sharing
- Temporary file distribution
- Small team file exchange
- Project document storage
- Media preview and sharing
- Folder packaging and archive download
- Rust web development learning
- Lightweight private file server experiments

## Possible future improvements

- Admin dashboard
- User storage quota
- Share expiration time
- Share password protection
- File search
- Recycle bin
- WebDAV support
- Resumable upload
- More detailed permission control
- External object storage backend
- Docker deployment files

## Sponsor ❤️

If RustDrive is useful to you, sponsorship is welcome and appreciated. It helps support maintenance, bug fixes, improvements, and future features.

| Type | Address |
| --- | --- |
| Bitcoin (BTC) | `bc1qxqfhumpqtnxrznkx9r4xsp8m6zsedtgusjns7p` |
| Ethereum (ETH) | `0x2d92f9e4d8ac7effa9cd7cd5eccd364cac7c201b` |
| BNB Smart Chain | `0x2d92f9e4d8ac7effa9cd7cd5eccd364cac7c201b` |
| USDT (Ethereum / ERC20) | `0x2d92f9e4d8ac7effa9cd7cd5eccd364cac7c201b` |

## License

This project is licensed under the GNU General Public License v3.0.


The author will continue to maintain this project. If you have any questions, please open an issue.


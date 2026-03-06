# GCS Download Skill - Architecture

## Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    GCS Download Skill                       │
└─────────────────────────────────────────────────────────────┘

┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│              │      │              │      │              │
│   config.py  │──────│   main.py    │──────│  tests/      │
│              │      │              │      │              │
└──────────────┘      └──────────────┘      └──────────────┘
  Dataclasses           Core Logic          Unit Tests
```

## Data Flow

```
User Input
    │
    ├─→ GCSDownloadInput (dataclass)
    │       ├─ gs_path: str
    │       ├─ local_dir: str
    │       └─ auto_discover: bool
    │
    ↓
┌─────────────────────────────────────┐
│  run(input)                         │
│                                     │
│  1. Connect to GCS                  │
│     └─→ gcsfs.GCSFileSystem()       │
│                                     │
│  2. Discover Files                  │
│     └─→ discover_files(fs, path)    │
│         ├─ Find qip_scores.parquet  │
│         ├─ Find config.json         │
│         └─ Find metadata.csv        │
│                                     │
│  3. Download Files                  │
│     └─→ download_file(gs, local)    │
│         ├─ Create local dir         │
│         ├─ Download with fs.get()   │
│         └─ Log progress             │
│                                     │
└─────────────────────────────────────┘
    │
    ↓
GCSDownloadOutput (dataclass)
    ├─ qip_scores_path: str
    ├─ config_path: str | None
    ├─ metadata_path: str | None
    ├─ download_dir: str
    └─ all_files: List[str]
```

## File Organization

```
skills/gcs_download/
│
├── __init__.py              # Public API exports
│   └── Exports: run, GCSDownloadInput, GCSDownloadOutput
│
├── config.py                # Data structures
│   ├── GCSDownloadInput     # Input configuration
│   └── GCSDownloadOutput    # Output results
│
├── main.py                  # Implementation
│   ├── discover_files()     # Pattern matching
│   ├── download_file()      # Single file download
│   └── run()                # Main entry point
│
├── skill.md                 # User documentation
├── README.md                # Quick start
├── ARCHITECTURE.md          # This file
│
└── tests/
    ├── __init__.py
    └── test_main.py         # Unit tests with mocks
```

## Function Call Flow

```
run(GCSDownloadInput)
    │
    ├─→ gcsfs.GCSFileSystem()
    │   └─ Authenticate to GCS
    │
    ├─→ os.makedirs(local_dir)
    │   └─ Create destination directory
    │
    ├─→ discover_files(fs, gs_path)
    │   │
    │   ├─→ fs.ls(directory)
    │   │   └─ List all files in GCS
    │   │
    │   └─→ Pattern matching
    │       ├─ *qip_scores*.parquet
    │       ├─ *config*.json
    │       └─ *metadata*.csv
    │
    ├─→ download_file(fs, gs_file, local_path)
    │   │   [Called for each discovered file]
    │   │
    │   ├─→ os.makedirs(parent_dir)
    │   ├─→ fs.get(gs_path, local_path)
    │   └─→ os.path.getsize()
    │
    └─→ GCSDownloadOutput(...)
        └─ Return results
```

## Error Handling Strategy

```
┌─────────────────────────────────────────────┐
│  Input Validation (config.py)              │
│                                             │
│  ✗ gs_path doesn't start with gs://        │
│    → ValueError("gs_path must start...")   │
│                                             │
│  ✗ qip_scores_path is None                 │
│    → ValueError("Failed to find...")       │
└─────────────────────────────────────────────┘
              │
              ↓
┌─────────────────────────────────────────────┐
│  Runtime Errors (main.py)                   │
│                                             │
│  ✗ GCS connection failed                   │
│    → Exception with details                │
│                                             │
│  ✗ Directory listing failed                │
│    → Exception with directory path         │
│                                             │
│  ✗ No qip_scores found                     │
│    → FileNotFoundError                     │
│                                             │
│  ⚠ Config/metadata missing                 │
│    → logger.warning() + continue           │
│                                             │
│  ✗ Download failed                         │
│    → Exception with file details           │
└─────────────────────────────────────────────┘
```

## Logging Strategy

```python
# Connection
logger.info("Connecting to GCS...")

# Discovery
logger.info(f"Discovering files in: {directory}")
logger.info(f"Found {len(all_files)} files in bucket")
logger.info(f"  ✓ Found qip_scores: {file_name}")

# Downloads
logger.info(f"  Downloading: {gs_file.split('/')[-1]}")
logger.info(f"    → {file_size / 1024 / 1024:.2f} MB")

# Warnings
logger.warning("  ⚠ No config file found (optional)")

# Success
logger.info(f"✓ Downloaded {len(downloaded_files)} file(s)")
```

## Testing Strategy

```
Unit Tests (test_main.py)
    │
    ├─→ test_gcs_download_input_validation()
    │   └─ Test input validation (gs:// prefix)
    │
    ├─→ test_discover_files()
    │   └─ Test pattern matching logic
    │
    ├─→ test_discover_files_fallback_parquet()
    │   └─ Test fallback to any .parquet
    │
    ├─→ test_discover_files_prefers_qip_scores()
    │   └─ Test qip_scores is preferred
    │
    ├─→ test_run_success()
    │   └─ Test full workflow with mocks
    │
    ├─→ test_run_missing_qip_scores()
    │   └─ Test error when no parquet found
    │
    └─→ test_gcs_download_output_validation()
        └─ Test output validation
```

## Dependencies

```
External:
├── gcsfs          # GCS filesystem access
├── os             # File operations
├── pathlib        # Path handling
└── logging        # Progress logging

Internal:
└── .config        # Dataclasses
```

## Usage Patterns

### Pattern 1: Simple Download
```python
from skills.gcs_download import run, GCSDownloadInput

result = run(GCSDownloadInput(
    gs_path='gs://bucket/path/'
))

print(result.qip_scores_path)
```

### Pattern 2: Custom Destination
```python
result = run(GCSDownloadInput(
    gs_path='gs://bucket/path/',
    local_dir='./my_custom_directory'
))
```

### Pattern 3: Error Handling
```python
try:
    result = run(GCSDownloadInput(gs_path=path))
except FileNotFoundError as e:
    print(f"Required file missing: {e}")
except ValueError as e:
    print(f"Invalid input: {e}")
except Exception as e:
    print(f"Download failed: {e}")
```

### Pattern 4: Check What Was Downloaded
```python
result = run(GCSDownloadInput(gs_path=path))

print(f"Downloaded {len(result.all_files)} files:")
for filepath in result.all_files:
    print(f"  - {filepath}")

if result.config_path:
    print("Config found, can parse experiment ID")
else:
    print("No config, will use defaults")
```

## Extension Points

Future enhancements could add:

1. **Progress Callbacks**
   ```python
   def on_progress(file, bytes_downloaded, total_bytes):
       print(f"{file}: {bytes_downloaded}/{total_bytes}")

   run(input, progress_callback=on_progress)
   ```

2. **Parallel Downloads**
   ```python
   # Download multiple files concurrently
   from concurrent.futures import ThreadPoolExecutor
   ```

3. **Resume Downloads**
   ```python
   # Check if file exists, resume if partial
   if os.path.exists(local_path):
       if not is_complete(local_path):
           resume_download(gs_path, local_path)
   ```

4. **Caching**
   ```python
   # Cache downloads by hash
   cache_key = hash(gs_path + file_mtime)
   ```

## Performance Considerations

- **Network**: Download time depends on file size and network speed
- **Storage**: Ensure sufficient local disk space
- **Memory**: Files streamed (not loaded entirely into memory)
- **Parallelization**: Downloads are sequential (could be parallelized)

## Security Considerations

- **Authentication**: Uses gcloud credentials or GOOGLE_APPLICATION_CREDENTIALS
- **Permissions**: Requires read access to GCS bucket
- **Validation**: Validates gs_path format to prevent path traversal
- **Logging**: Doesn't log sensitive data (tokens, credentials)

---

This architecture provides a solid foundation for the first skill in our modular recall analysis system!

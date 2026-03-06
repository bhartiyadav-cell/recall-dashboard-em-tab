# How Recursive Subdirectory Search Works

## Visual Flow

```
Your GCS Bucket
gs://p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383/
│
├─→ [List Items] ──────────────────────────┐
│                                          │
├── sample-5000/  (directory)  ───→ [Detect: Is Directory?]
│                                          │
│                                      YES │
│                                          ↓
│                                   [List Files Inside]
│                                          │
│                                          ├─ qip_scores.parquet     ✓
│                                          ├─ config_crawl_preso.json ✓
│                                          └─ metadata.csv           ✓
│
├── sample-1000/  (directory)  ───→ [Detect: Is Directory?]
│                                          │
│                                      YES │
│                                          ↓
│                                   [List Files Inside]
│                                          │
│                                          └─ other.parquet          ✓
│
└── readme.txt    (file)       ───→ [Detect: Is Directory?]
                                           │
                                       NO  │
                                           ↓
                                    [Add to file list] ✓
```

## Step-by-Step Process

### Step 1: List Top-Level Items
```python
items = fs.ls('p0y01cc/l1_recall_analysis/nlf_v2_v3/1770620383')
# Returns: ['sample-5000', 'sample-1000', 'readme.txt']
```

### Step 2: Check Each Item
```python
for item in items:
    is_directory = fs.info(item).get('type') == 'directory'
```

### Step 3a: If Directory → Search Inside
```python
if is_directory:
    subdir_files = fs.ls(item)  # List files in subdirectory
    all_files.extend(subdir_files)  # Add to collection
```

### Step 3b: If File → Add Directly
```python
else:
    all_files.append(item)  # Add file to collection
```

### Step 4: Search All Files
```python
for file_path in all_files:
    if 'qip_scores' in file_name and file_name.endswith('.parquet'):
        discovered['qip_scores'] = file_path  # Found it!
```

## Code Comparison

### Before (Non-Recursive)
```python
# Only searched top level
all_files = fs.ls(directory)  # Gets: ['sample-5000', 'sample-1000']
# Never looks inside sample-5000/
```

### After (Recursive)
```python
# Searches subdirectories
items = fs.ls(directory)  # Gets: ['sample-5000', 'sample-1000']

for item in items:
    if is_directory(item):
        subdir_files = fs.ls(item)  # Gets: ['sample-5000/qip_scores.parquet', ...]
        all_files.extend(subdir_files)  # Now includes files from subdirectories!
```

## Real Example with Your Data

### Your Directory Structure
```
1770620383/
├── sample-5000/
│   ├── qip_scores.parquet           ← Target file!
│   ├── config_crawl_preso.json      ← Target file!
│   └── metadata.csv                 ← Target file!
└── sample-1000/
    └── other_data.parquet
```

### What Happens

1. **List top level:**
   ```
   Found 2 items: ['sample-5000', 'sample-1000']
   ```

2. **Check sample-5000:**
   ```
   → Is directory? YES
   → List inside: ['qip_scores.parquet', 'config_crawl_preso.json', 'metadata.csv']
   → Add to search list ✓
   ```

3. **Check sample-1000:**
   ```
   → Is directory? YES
   → List inside: ['other_data.parquet']
   → Add to search list ✓
   ```

4. **Search all files:**
   ```
   Total files: 4
   → qip_scores.parquet       ✓ MATCH! (has 'qip_scores' and '.parquet')
   → config_crawl_preso.json  ✓ MATCH! (has 'config' and '.json')
   → metadata.csv             ✓ MATCH! (has 'metadata' and '.csv')
   → other_data.parquet       ✗ (already found qip_scores)
   ```

5. **Download matched files:**
   ```
   Downloading: qip_scores.parquet from sample-5000/
   Downloading: config_crawl_preso.json from sample-5000/
   Downloading: metadata.csv from sample-5000/
   ```

## Why This Matters

### Problem
Files are often organized in subdirectories for different sample sizes or experiments:
- `sample-5000/` - Full dataset
- `sample-1000/` - Smaller sample for testing
- `sample-100/` - Tiny sample for debugging

### Solution
The skill automatically finds files regardless of which subdirectory they're in!

### Flexibility
```python
# Search everywhere (default)
result = run(GCSDownloadInput(gs_path='...', recursive=True))

# Only top level (if you know files are there)
result = run(GCSDownloadInput(gs_path='...', recursive=False))
```

## Key Code Snippets

### Directory Detection
```python
try:
    item_info = fs.info(item)
    is_directory = item_info.get('type') == 'directory'
except:
    # Fallback: check if it has a file extension
    is_directory = '.' not in item.split('/')[-1]
```

### Subdirectory Search
```python
if is_directory and recursive:
    subdir_name = item.split('/')[-1]
    logger.info(f"  Searching subdirectory: {subdir_name}")

    try:
        subdir_files = fs.ls(item)
        all_files.extend(subdir_files)
        logger.info(f"    Found {len(subdir_files)} files in {subdir_name}")
    except Exception as e:
        logger.warning(f"    Could not list {subdir_name}: {e}")
```

### File Pattern Matching
```python
for file_path in all_files:
    file_name = file_path.split('/')[-1].lower()

    if 'qip_scores' in file_name and file_name.endswith('.parquet'):
        discovered['qip_scores'] = f"gs://{file_path}"
        logger.info(f"  ✓ Found qip_scores: {file_name} (in {'/'.join(file_path.split('/')[-2:])})")
```

## Benefits

✅ **Automatic** - No need to specify subdirectory name
✅ **Flexible** - Works with any subdirectory structure
✅ **Robust** - Handles missing subdirectories gracefully
✅ **Informative** - Logs which subdirectory files came from
✅ **Configurable** - Can disable if needed

---

**Now you understand how it works!** Run `python test_gcs_download.py` to see it in action! 🚀

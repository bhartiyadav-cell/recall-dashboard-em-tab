# Extracting Config from Recall Analysis Email

## The Discovery 🎯

After analyzing the recall analysis bash script (`run.sh`), I found that **the config IS in the email** - it's embedded in HTML format!

### How the Email is Generated

The bash script does this (lines near the end):

```bash
echo "<h2 style=\"color:#0072ce;\">Config File</h2>" >> "$output_file"
echo "<pre style=\"font-family: monospace; white-space: pre; font-size: 8pt; line-height: 0.8;\">" >> "$output_file"
while IFS= read -r line; do
  echo "$line<br />" >> "$output_file"
done < "$CONFIG_FOLDER/config_crawl_preso.json"
echo '</pre>' >> "$output_file"
```

Then it sends the email:

```python
EMAIL_BODY='<p>All relevant intermediate files are uploaded at: <a href="' + '$USER_GS_BUCKET' + '">' + '$USER_GS_BUCKET' + '</a></p><br />'
EMAIL_BODY+=open('$output_file').read()

Utils.send_mail(
    mail_to=USER_EMAILS,
    mail_to_cc=['l1_recall_analysis@email.wal-mart.com'],
    subject='Defect Rate Analysis Report - ' + '$(basename $CONFIG_FOLDER)',
    body_html=EMAIL_BODY
)
```

### What This Means

The email contains:
1. **GCS bucket path** - In a link like `gs://k0k01ls/l1_recall_analysis/...`
2. **Full config JSON** - Embedded in a `<pre>` tag with `<br/>` line breaks

Example HTML structure:
```html
<p>All relevant intermediate files are uploaded at:
  <a href="gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932">
    gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932
  </a>
</p>

<!-- ... metrics tables ... -->

<h2 style="color:#0072ce;">Config File</h2>
<pre style="font-family: monospace; white-space: pre; font-size: 8pt; line-height: 0.8;">
{<br />
  "comments": "L1 Ranker AB Test",<br />
  "engines": {<br />
    "control": {<br />
      "host": "http://preso-usgm-wcnp.prod.walmart.com",<br />
      "request_params": {<br />
        "stores": "4108",<br />
        "zipcode": "94086",<br />
        "ptss": "l1_ranker_disable_bfs:on"<br />
      }<br />
    },<br />
    "ltr_ab_candidates": {<br />
      "host": "http://preso-usgm-wcnp.prod.walmart.com",<br />
      "request_params": {<br />
        "ptss": "use_variant_solr:on;l1_ranker_disable_bfs:on"<br />
      }<br />
    }<br />
  }<br />
}<br />
</pre>
```

## The Solution

I've updated the `extract_json_from_email()` function to:
1. Look for `<pre>` tags in the HTML email
2. Remove the `<br/>` tags (replace with newlines)
3. Clean up HTML entities (`&lt;`, `&gt;`, etc.)
4. Extract and validate the JSON

### New Function

```python
def extract_json_from_html_pre(html_text: str) -> Optional[str]:
    """
    Extract JSON from HTML <pre> tag.

    The recall analysis script embeds the config JSON in a <pre> tag with <br/> line breaks.
    """
    # Look for <pre> tag with config
    pre_pattern = r'<pre[^>]*>(.*?)</pre>'
    matches = re.findall(pre_pattern, html_text, re.DOTALL | re.IGNORECASE)

    for match in matches:
        # Remove HTML tags and entities
        content = match
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)  # Replace <br/> with newlines
        content = re.sub(r'<[^>]+>', '', content)  # Remove any other HTML tags
        content = content.replace('&lt;', '<').replace('&gt;', '>').replace('&quot;', '"').replace('&amp;', '&')

        # Check if it looks like a config (has "engines" or "comments")
        if '"engines"' in content or '"comments"' in content:
            try:
                json.loads(content)
                return content
            except json.JSONDecodeError:
                content = content.strip()
                try:
                    json.loads(content)
                    return content
                except json.JSONDecodeError:
                    continue

    return None
```

## Testing

Run the test script:
```bash
python test_html_pre_extraction.py
```

This will:
1. Read the email file
2. Decode base64 content
3. Extract GCS path
4. Extract JSON config from `<pre>` tag
5. Validate the JSON
6. Show the engines and parameters

## Usage

Now you can use the email-based pipeline:

```bash
# Option 1: From email file
python run_from_email.py --email emails/Defect\ Rate\ Analysis\ Report\ -\ ltr_ab_candidates.eml

# Option 2: Still works - provide GCS and config separately
python run_pipeline.py \
  --gcs-path "gs://k0k01ls/l1_recall_analysis/ltr_ab_candidates/1773649932" \
  --config-file experiment_config.json
```

The email-based approach should now work because:
1. ✅ GCS path is in the email (we already extracted this successfully)
2. ✅ Config JSON is in the email (embedded in `<pre>` tag - now we can extract it!)

## Why This is Better

- **Fully automated** - Just provide the email file
- **No manual copy-paste** - Extracts both GCS path and config automatically
- **Matches your workflow** - You already receive these emails from the recall analysis pipeline
- **Single source of truth** - The email contains everything needed to reproduce the analysis

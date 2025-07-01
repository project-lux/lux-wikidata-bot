# Wikidata LUX Uploader

This script batch-adds LUX URIs to Wikidata items via the Wikidata API using OAuth authentication. It supports retrying failed uploads, checking for existing claims, and logging all activity.

## Features

- Adds a custom property (`P13591` for LUX URIs) to Wikidata items
- Skips items that already have the LUX URI
- Handles CSRF token exchange automatically
- Logs successful and failed uploads
- Supports retrying from previous failure logs
- Uses a `.env` file to securely manage credentials

## Requirements

- Python 3.7+
- A registered bot account on Wikidata with OAuth credentials
- Your LUX URIs and QIDs in a CSV file

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/project-lux/lux-wikidata-bot.git
   cd lux-wikidata-bot
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the root directory:

   ```env
   CONSUMER_KEY=your_consumer_key
   CONSUMER_SECRET=your_consumer_secret
   ACCESS_TOKEN=your_access_token
   ACCESS_SECRET=your_access_secret
   ```

   > **Note:** Never commit this file. `.env` is included in `.gitignore`.

## Usage

1. Prepare your input CSV:
   - Must have two columns: `QID`, `LUX_URI`
   - Example:
     ```
     Q12345,https://lux.collections.yale.edu/data/person/abc123
     ```

2. Run the script:

   ```bash
   python batch_wiki_lux.py
   ```

3. After the run, check:
   - `lux_upload_success.csv` – records that were successfully updated
   - `lux_upload_failures.csv` – records that failed to upload

4. To retry failures, change the input file in the config section of `batch_wiki_lux.py`:

   ```python
   INPUT_FILE = "lux_upload_failures.csv"
   SUCCESS_FILE = "lux_retry_success.csv"
   FAILURE_FILE = "lux_retry_failures.csv"
   ```

## Notes

- Uses a short sleep between requests (`CHUNK_SLEEP`) to avoid hitting rate limits.
- Logs are written to `lux_upload.log` with timestamped entries.
- Handles common network and API errors with graceful logging.

## License

This project is licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](LICENSE). You may use, share, and adapt the code with attribution.

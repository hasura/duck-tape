# **🦆 duck-tape**

*Tapes all your disparate data sources together into a single, queryable DuckDB catalog.*

duck-tape is a powerful Python command-line utility that discovers data from a wide array of local files, remote URIs, and databases, and creates a unified, high-performance DuckDB database file from them. It acts as a "data catalog builder," making it easy to query all your data in one place using SQL.

It's designed to be the perfect companion for tools like the Hasura DuckDB Native Data Connector (NDC), providing a pre-built, queryable database file that's always ready for introspection.

## **What it Does**

duck-tape uses a sophisticated, performance-first architecture. It automatically makes a **smart choice** for each data source based on a configurable "sweet spot" to balance performance, latency, and storage costs.

1. **Direct View (for Small or Very Large Files):** For simple formats like CSV and JSON that are outside a configurable size range, it creates a direct VIEW.  
   * **Small files:** Prioritizes low data latency and avoids unnecessary processing.  
   * **Very large files:** Avoids the high storage cost of duplicating massive datasets.  
2. **Convert-to-Parquet (for the "Sweet Spot" and Complex Files):** For files that are in the ideal size range for performance gains, or for complex formats (Excel, PDF, etc.), it uses DuckDB or Pandas to parse them and transform them into the highly efficient **Parquet** format. It then creates a VIEW pointing to this intermediate Parquet file. This ensures maximum query performance where it matters most.

This hybrid approach ensures that all queries are fast and efficient while remaining flexible and cost-effective. The final .duckdb catalog file contains only VIEWs, keeping it incredibly small and portable.

## **Features**

* **Advanced Configuration:** Uses a layered configuration system (CLI \> Environment Variables \> YAML File \> Defaults).  
* **S3 Integration:** Natively reads sources from and writes intermediate files to S3 buckets.  
* **Smart Strategy Selection:** Automatically chooses the best processing strategy (direct view vs. convert-to-parquet) based on file type and a configurable size range.  
* **Smart Table Discovery:**  
  * **For Excel:** Intelligently finds and extracts multiple, distinct data tables from a single sheet, even if they are offset.  
  * **For XML:** Traverses the document tree to find and extract nested arrays of data, handling complex, hierarchical files automatically.  
* **Multi-Format Support:** Ingests data from a huge variety of sources:  
  * CSV, JSON, Parquet, Feather/Arrow  
  * Excel (including all sheets)  
  * **PDF (Optional):** Extracts tables if dependencies are installed.  
  * HTML (scrapes tables from local files or live URLs)  
  * Markdown (extracts tables)  
  * XML, YAML (including multi-document files)  
  * Avro, ORC  
* **Direct Database Connection:** Connects directly to **SQLite** databases and creates views for all tables.  
* **Live Reloading (Watch Mode):** An optional \--watch mode automatically monitors local files and polls remote sources (HTTP/S, S3), updating the DuckDB catalog in real-time.

## **Setup & Installation**

**1\. Python Libraries**

You can install all necessary Python packages from the requirements.txt file:

pip install \-r requirements.txt

**2\. Optional: Enabling PDF Support**

PDF table extraction requires extra dependencies. If you need to process PDFs, follow these steps:

* **Install the Python library:** Uncomment camelot-py\[cv\] in your requirements.txt file, or run:  
  pip install "camelot-py\[cv\]"

* **Install System Dependencies:** camelot requires Ghostscript.  
  * **On Debian/Ubuntu:**  
    sudo apt-get update && sudo apt-get install \-y ghostscript python3-tk

  * **On macOS (with Homebrew):**  
    brew install ghostscript

If you run duck-tape and these dependencies are not met, it will print a warning and safely skip any PDF files it finds.

## **Configuration**

duck-tape uses a layered configuration system. Settings are loaded in the following order of priority:

1. **Command-Line Arguments** (e.g., \--config)  
2. **Environment Variables** (e.g., DUCKTAPE\_OUTPUT\_DB\_FILE)  
3. **YAML Configuration File** (e.g., config.yaml)  
4. **Script Defaults**

### **Example config.yaml**

Create a config.yaml file to define your settings.

\# List of sources to process. Can be local globs, URLs, or DB connection strings.  
sources:  
  \- 'data/\*\*/\*'  
  \- 's3://my-data-bucket/raw\_files/sales.csv'  
  \- 'https://www.w3schools.com/html/html\_tables.asp'

\# Path for the final DuckDB catalog file.  
output\_db\_file: 'catalog.db'

\# Directory to store intermediate Parquet files. Can be a local path or an S3 URI.  
intermediate\_dir: 's3://my-data-bucket/ducktape\_cache'

\# Debounce delay in seconds for the local file watcher.  
debounce\_seconds: 2.0

\# How often (in seconds) to poll remote sources (HTTP/S, S3) in watch mode.  
\# Set to 0 to disable remote polling.  
polling\_interval\_seconds: 60

\# "Sweet Spot" size range (in MB) for Parquet conversion.  
convert\_to\_parquet\_min\_mb: 50  
convert\_to\_parquet\_max\_mb: 1024

\# S3-specific settings. Credentials should be set via environment variables.  
s3:  
  region: 'us-east-1'  
  endpoint: null \# Optional: for S3-compatible storage like MinIO  
  url\_style: 'vhost' \# or 'path'

\# Settings for Smart Table Discovery in Excel files.  
excel\_table\_discovery:  
  min\_rows: 3  
  min\_cols: 2

\# Settings for Smart Table Discovery in XML files.  
xml\_table\_discovery:  
  min\_records: 3 \# Min repeating elements to be considered a table  
  max\_depth: 5   \# How deep to search in the XML tree

### **Environment Variables**

| Environment Variable | Overrides Setting | Format |
| :---- | :---- | :---- |
| DUCKTAPE\_SOURCES | sources | Comma-separated string |
| DUCKTAPE\_OUTPUT\_DB\_FILE | output\_db\_file | String (file path) |
| DUCKTAPE\_INTERMEDIATE\_DIR | intermediate\_dir | String (directory path or S3 URI) |
| DUCKTAPE\_POLLING\_INTERVAL\_SECONDS | polling\_interval\_seconds | Integer |
| DUCKTAPE\_S3\_REGION | s3.region | String |
| **AWS\_ACCESS\_KEY\_ID** | S3 Authentication | AWS Access Key |
| **AWS\_SECRET\_ACCESS\_KEY** | S3 Authentication | AWS Secret Key |
| **AWS\_SESSION\_TOKEN** | S3 Authentication (Optional) | AWS Session Token |

## **Usage**

duck-tape is a command-line tool with two primary modes.

**1\. One-Time Build (Default)**

This performs a full, clean build of the DuckDB catalog.

\# Use the default config.yaml  
python duck\_tape.py \--build

\# Specify a different config file  
python duck\_tape.py \--config prod\_config.yaml \--build

**2\. Watch Mode**

For local development or in production, you can run the watcher. It will perform an initial build and then continue running, monitoring local source directories and polling remote sources, updating the DuckDB catalog in real-time.

python duck\_tape.py \--watch

\# Use a different config file in watch mode  
python duck\_tape.py \--config dev\_config.yaml \--watch

## **Limitations and Considerations**

While duck-tape is powerful, it's important to understand its behavior in large-scale scenarios.

* **Large Number of Files:** The initial build process scans all files to generate the catalog. If your sources contain tens of thousands of files, this initial scan can be time-consuming. The watcher, however, will only process changes incrementally after the initial build.  
* **Very Large Individual Files:** When a file is converted to Parquet, it must be fully read into memory by Pandas or DuckDB. For extremely large files (e.g., 50GB+), this can be memory-intensive. This is why the CONVERT\_TO\_PARQUET\_MAX\_MB setting is useful for avoiding this on massive files.  
* **Remote Polling:** The watcher polls remote sources on a timer. It is not event-driven, so changes will only be detected after the polling\_interval\_seconds has elapsed. It also cannot detect when a remote file is deleted; a manual \--build is required to clean up views for deleted remote sources.  
* **Complex Formats (PDF/HTML/XML):** Table extraction from formats designed for presentation or complex hierarchies is not always perfect. duck-tape uses powerful libraries and smart heuristics to do its best, but results can vary depending on the structure of the source file. Always validate the resulting schema for these types of sources.

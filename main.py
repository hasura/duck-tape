import duckdb
import pandas as pd
import glob
from pathlib import Path
import yaml
import re
import os
import time
import argparse
from urllib.parse import urlparse
import markdown
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Timer, Thread, Event
import requests
import xml.etree.ElementTree as ET
from collections import Counter
import shutil

# --- Optional Dependency Handling for PDF Support ---
PDF_SUPPORT_ENABLED = False
try:
    import camelot
    if shutil.which("ghostscript"):
        PDF_SUPPORT_ENABLED = True
    else:
        print("Warning: The 'camelot-py' library is installed, but Ghostscript is not found in your system's PATH. PDF processing will be disabled. Please install Ghostscript to enable PDF support.")
except ImportError:
    print("Warning: 'camelot-py' is not installed. PDF processing will be disabled. To enable, run: pip install 'camelot-py[cv]'")


# --- Default Configuration ---
DEFAULT_CONFIG = {
    'sources': ['data/**/*'],
    'output_db_file': 'data_catalog.duckdb',
    'intermediate_dir': 'intermediate_data',
    'debounce_seconds': 2.0,
    'polling_interval_seconds': 60, # Set to 0 to disable remote polling
    'convert_to_parquet_min_mb': 50,
    'convert_to_parquet_max_mb': 1024,
    's3': {
        'region': None,
        'endpoint': None,
        'url_style': 'vhost'
    },
    'excel_table_discovery': {
        'min_rows': 3,
        'min_cols': 2
    },
    'xml_table_discovery': {
        'min_records': 3,
        'max_depth': 5
    }
}

SUPPORTED_EXTENSIONS = [
    '.csv', '.json', '.xlsx', '.xls', '.xml', '.yaml', '.yml',
    '.html', '.htm', '.md', '.parquet', '.arrow', '.feather',
    '.avro', '.orc'
]
if PDF_SUPPORT_ENABLED:
    SUPPORTED_EXTENSIONS.append('.pdf')

def load_config(config_path):
    """Loads configuration from YAML file and overrides with environment variables."""
    config = DEFAULT_CONFIG.copy()

    if os.path.exists(config_path):
        print(f"-> Loading configuration from '{config_path}'")
        with open(config_path, 'r') as f:
            yaml_config = yaml.safe_load(f)
            if yaml_config:
                config.update(yaml_config)
    else:
        print(f"-> Config file '{config_path}' not found. Using defaults and environment variables.")

    env_overrides = {
        'sources': os.getenv('DUCKTAPE_SOURCES'),
        'output_db_file': os.getenv('DUCKTAPE_OUTPUT_DB_FILE'),
        'intermediate_dir': os.getenv('DUCKTAPE_INTERMEDIATE_DIR'),
        's3_region': os.getenv('DUCKTAPE_S3_REGION'),
        'polling_interval_seconds': os.getenv('DUCKTAPE_POLLING_INTERVAL_SECONDS'),
    }

    if env_overrides['sources']:
        config['sources'] = [s.strip() for s in env_overrides['sources'].split(',')]
    if env_overrides['output_db_file']:
        config['output_db_file'] = env_overrides['output_db_file']
    if env_overrides['intermediate_dir']:
        config['intermediate_dir'] = env_overrides['intermediate_dir']
    if env_overrides['s3_region']:
        config['s3']['region'] = env_overrides['s3_region']
    if env_overrides['polling_interval_seconds']:
        config['polling_interval_seconds'] = int(env_overrides['polling_interval_seconds'])

    return config


def get_db_connection(config):
    """Creates and configures a DuckDB connection."""
    db = duckdb.connect(database=config['output_db_file'], read_only=False)
    db.execute("INSTALL httpfs; LOAD httpfs;")
    if config.get('s3', {}).get('region'):
        db.execute(f"SET s3_region='{config['s3']['region']}';")
    if config.get('s3', {}).get('endpoint'):
        db.execute(f"SET s3_endpoint='{config['s3']['endpoint']}';")
    if config.get('s3', {}).get('url_style'):
        db.execute(f"SET s3_url_style='{config['s3']['url_style']}';")
    return db


def sanitize_name(name):
    """Sanitizes a string to be a valid SQL identifier."""
    name = re.sub(r'[\\/\s.-]', '_', name)
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    name = name.lower()
    return name.strip('_')


def generate_table_name(source, optional_part=None):
    """Creates a descriptive and unique table name from a file path or URL."""
    source_str = str(source)
    if source_str.startswith(('http://', 'https://', 's3://', 'sqlite://')):
        parsed_url = urlparse(source_str)
        path_parts = parsed_url.path.strip('/').split('/')
        stem = Path(path_parts[-1]).stem if path_parts[-1] else "index"
        parts = [parsed_url.netloc] + path_parts[:-1] + [stem]
    else:
        p = Path(source)
        parts = list(p.parts[:-1]) + [p.stem]
    if optional_part:
        parts.append(str(optional_part))
    return '_'.join(sanitize_name(part) for part in parts if part)


def find_tables_in_sheet(df, config):
    """
    Finds and extracts one or more distinct data tables from a raw Excel sheet DataFrame.
    """
    tables = []
    min_rows = config['excel_table_discovery']['min_rows']
    min_cols = config['excel_table_discovery']['min_cols']

    mask = df.notna()

    while mask.any().any():
        start_row = mask.any(axis=1).idxmax()
        start_col = mask.loc[start_row].idxmax()

        end_row = start_row
        for i in range(start_row, len(df)):
            if not mask.loc[i].any():
                break
            end_row = i

        sub_mask = mask.loc[start_row:end_row]
        end_col = start_col
        for j in range(start_col, len(df.columns)):
            if not sub_mask.iloc[:, j - start_col].any():
                break
            end_col = j

        table_df = df.iloc[start_row:end_row + 1, start_col:end_col + 1].copy()

        if table_df.shape[0] >= min_rows and table_df.shape[1] >= min_cols:
            new_header = table_df.iloc[0]
            table_df = table_df[1:]
            table_df.columns = new_header
            table_df.columns.name = None
            table_df = table_df.reset_index(drop=True)
            tables.append(table_df)

        mask.iloc[start_row:end_row + 1, start_col:end_col + 1] = False

    return tables


def find_tables_in_xml(root, config):
    """
    Recursively finds and extracts tabular data from an XML tree.
    """
    tables = []
    min_records = config['xml_table_discovery']['min_records']
    max_depth = config['xml_table_discovery']['max_depth']

    def recurse(element, depth):
        if depth > max_depth:
            return

        # Heuristic: A table is a node with many children that have the same tag.
        if len(element) >= min_records:
            tags = [child.tag for child in element]
            tag_counts = Counter(tags)
            most_common_tag, count = tag_counts.most_common(1)[0]

            # If the most common tag makes up > 80% of children, it's likely a table
            if count / len(element) > 0.8:
                records = []
                for child in element:
                    if child.tag == most_common_tag:
                        record = {sub.tag: sub.text for sub in child}
                        records.append(record)

                if records:
                    tables.append((pd.DataFrame(records), element.tag))
                return  # Stop recursing down this branch once we've found a table

        # If no table found at this level, recurse into children
        for child in element:
            recurse(child, depth + 1)

    recurse(root, 0)
    return tables


def process_source_file(source_path, db_connection, config):
    """
    Processes a single source file, deciding whether to create a direct view
    or convert it to an intermediate Parquet file based on type and size.
    """
    is_local = not str(source_path).startswith(('http', 's3'))
    if is_local and (not os.path.exists(source_path) or os.path.isdir(source_path)):
        return

    extension = Path(urlparse(source_path).path).suffix.lower()
    if extension not in SUPPORTED_EXTENSIONS:
        return

    strategy = 'direct_view'
    if extension not in ['.csv', '.json', '.parquet', '.feather', '.arrow']:
        strategy = 'convert_to_parquet'
    elif extension in ['.csv', '.json']:
        file_size_mb = 0
        try:
            if is_local:
                file_size_mb = os.path.getsize(source_path) / (1024 * 1024)
            else:
                response = requests.head(source_path, allow_redirects=True)
                file_size_mb = int(response.headers.get('content-length', 0)) / (1024 * 1024)

            if config['convert_to_parquet_min_mb'] <= file_size_mb <= config['convert_to_parquet_max_mb']:
                strategy = 'convert_to_parquet'
                print(f"  -> File '{source_path}' is in performance sweet spot. Converting to Parquet.")
            elif file_size_mb > config['convert_to_parquet_max_mb']:
                print(f"  -> File '{source_path}' is very large. Using direct view to avoid high storage cost.")
            else:
                print(f"  -> File '{source_path}' is small. Using direct view for low latency.")
        except Exception:
            print(f"  -> Could not determine size of remote file '{source_path}'. Defaulting to direct view.")

    try:
        if strategy == 'direct_view':
            view_name = generate_table_name(source_path)
            print(f"  -> Creating direct VIEW '{view_name}' for: {source_path}")
            reader_function = \
            {'.csv': 'read_csv_auto', '.json': 'read_json_auto', '.parquet': 'read_parquet', '.arrow': 'read_ipc',
             '.feather': 'read_ipc'}[extension]
            resolved_path = str(Path(source_path).resolve()) if is_local else source_path
            db_connection.execute(
                f"""CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM {reader_function}('{resolved_path}');""")

        elif strategy == 'convert_to_parquet':
            dataframes = []
            if extension in ['.csv', '.json']:
                reader_func = {'csv': 'read_csv_auto', 'json': 'read_json_auto'}[extension[1:]]
                df = db_connection.execute(f"SELECT * FROM {reader_func}('{source_path}')").fetchdf()
                dataframes.append((df, None))
            elif extension in ['.xlsx', '.xls']:
                print(f"  -> Discovering tables in Excel file: {source_path}")
                xls = pd.ExcelFile(source_path)
                for sheet_name in xls.sheet_names:
                    raw_df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                    discovered_tables = find_tables_in_sheet(raw_df, config)
                    for i, table_df in enumerate(discovered_tables):
                        optional_part = f"{sheet_name}_table{i}"
                        dataframes.append((table_df, optional_part))
            elif extension == '.xml':
                print(f"  -> Discovering tables in XML file: {source_path}")
                tree = ET.parse(source_path)
                root = tree.getroot()
                discovered_tables = find_tables_in_xml(root, config)
                for df, parent_tag in discovered_tables:
                    dataframes.append((df, parent_tag))
            elif extension in ['.avro', '.orc']:
                if extension == '.avro':
                    df = pd.read_avro(source_path)
                elif extension == '.orc':
                    df = pd.read_orc(source_path)
                dataframes.append((df, None))
            elif extension in ['.html', '.htm', '.md']:
                html_content = markdown.markdown(open(source_path, encoding='utf-8').read(), extensions=[
                    'tables']) if extension == '.md' and is_local else source_path
                all_html_tables = pd.read_html(html_content)
                for i, df in enumerate(all_html_tables):
                    if df.shape[0] >= 3 and df.shape[1] >= 2: dataframes.append((df, f"table{i}"))
            elif extension == '.pdf':
                if PDF_SUPPORT_ENABLED:
                    pdf_tables = camelot.read_pdf(source_path, pages='all', flavor='lattice')
                    for i, table in enumerate(pdf_tables): dataframes.append((table.df, f"page{table.page}_table{i}"))
                else:
                    print(f"  -> Skipping PDF file because PDF support is not enabled: {source_path}")

            for df, optional_part in dataframes:
                if df.empty:
                    print(f"    -> Skipping empty or invalid table from source: {source_path} ({optional_part or ''})")
                    continue

                view_name = generate_table_name(source_path, optional_part)
                intermediate_path_str = os.path.join(config['intermediate_dir'], f"{view_name}.parquet")
                if config['intermediate_dir'].startswith('s3://'):
                    intermediate_path_str = f"{config['intermediate_dir']}/{view_name}.parquet"

                print(f"    -> Saving intermediate Parquet file: {intermediate_path_str}")
                df.to_parquet(intermediate_path_str, index=False)

                resolved_intermediate_path = str(
                    Path(intermediate_path_str).resolve()) if not intermediate_path_str.startswith(
                    's3://') else intermediate_path_str
                print(f"    -> Creating VIEW '{view_name}' for intermediate file")
                db_connection.execute(
                    f"""CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM read_parquet('{resolved_intermediate_path}');""")

    except Exception as e:
        print(f"  ❌ Error processing source {source_path}: {e}")


def delete_source_views(source_path, db_connection, config):
    """Deletes all views and intermediate files associated with a source."""
    print(f"  -> Deleting artifacts for: {source_path}")
    base_name = generate_table_name(source_path)

    intermediate_dir = config['intermediate_dir']
    if intermediate_dir.startswith('s3://'):
        print("  -> S3 cleanup not fully implemented. Dropping views only.")
    else:
        for f in Path(intermediate_dir).glob(f"{base_name}*.parquet"):
            try:
                view_name = f.stem
                print(f"    -> Dropping VIEW: {view_name}")
                db_connection.execute(f'DROP VIEW IF EXISTS "{view_name}";')
                os.remove(f)
                print(f"    -> Deleted intermediate file: {f}")
            except Exception as e:
                print(f"  ❌ Error during cleanup for {f}: {e}")

    db_connection.execute(f'DROP VIEW IF EXISTS "{base_name}";')


class DebouncedChangeHandler(FileSystemEventHandler):
    """A debounced handler that processes a batch of file changes after a quiet period."""

    def __init__(self, config, db_connection):
        self.debounce_timer = None
        self.changed_files = set()
        self.deleted_files = set()
        self.config = config
        self.db_connection = db_connection

    def dispatch_event(self, event):
        if not event.is_directory:
            if event.event_type == 'deleted':
                self.deleted_files.add(event.src_path)
                if event.src_path in self.changed_files: self.changed_files.remove(event.src_path)
            else:
                self.changed_files.add(event.src_path)
                if event.src_path in self.deleted_files: self.deleted_files.remove(event.src_path)

            if self.debounce_timer: self.debounce_timer.cancel()
            self.debounce_timer = Timer(self.config['debounce_seconds'], self.process_changes)
            self.debounce_timer.start()

    def on_any_event(self, event):
        self.dispatch_event(event)

    def process_changes(self):
        print(f"\n--- Debounced Event: Processing {len(self.changed_files) + len(self.deleted_files)} changes ---")
        for f in self.deleted_files: delete_source_views(f, self.db_connection, self.config)
        for f in self.changed_files: process_source_file(f, self.db_connection, self.config)
        self.changed_files.clear()
        self.deleted_files.clear()
        print("--- Finished processing batch. Waiting for next change... ---")

def poll_remote_sources(remote_uris, db_connection, config, stop_event):
    """Periodically checks remote sources for changes."""
    if not remote_uris:
        return

    print(f"-> Starting remote poller for {len(remote_uris)} URIs (interval: {config['polling_interval_seconds']}s)")
    last_seen_etags = {}

    while not stop_event.is_set():
        for uri in remote_uris:
            try:
                if uri.startswith('s3://'):
                    # S3 ETag checking requires s3fs
                    import s3fs
                    fs = s3fs.S3FileSystem()
                    current_etag = fs.info(uri).get('ETag')
                else: # HTTP/S
                    response = requests.head(uri, allow_redirects=True, timeout=5)
                    current_etag = response.headers.get('ETag') or response.headers.get('Last-Modified')

                if current_etag and last_seen_etags.get(uri) != current_etag:
                    print(f"\n[REMOTE CHANGE DETECTED] {uri}")
                    process_source_file(uri, db_connection, config)
                    last_seen_etags[uri] = current_etag

            except Exception as e:
                print(f"  -> Warning: Could not poll remote source {uri}. Error: {e}")

        stop_event.wait(config['polling_interval_seconds'])


def build_catalog(config):
    """Performs a full, clean build of the DuckDB catalog."""
    print("--- Starting Full Catalog Build ---")

    if not config['intermediate_dir'].startswith('s3://'):
        Path(config['intermediate_dir']).mkdir(parents=True, exist_ok=True)
    Path(config['output_db_file']).parent.mkdir(parents=True, exist_ok=True)

    if os.path.exists(config['output_db_file']):
        os.remove(config['output_db_file'])
        print(f"Removed existing database file: {config['output_db_file']}")

    db_connection = get_db_connection(config)

    local_globs, remote_uris, db_uris = [], [], []
    for source in config['sources']:
        if source.startswith('sqlite://'):
            db_uris.append(source)
        elif source.startswith(('http://', 'https://', 's3://')):
            remote_uris.append(source)
        else:
            local_globs.append(source)

    local_files = []
    for pattern in local_globs:
        local_files.extend(glob.glob(pattern, recursive=True))

    all_file_sources = [f for f in local_files if not Path(f).is_dir()] + remote_uris

    print(f"Found {len(all_file_sources)} files/URIs and {len(db_uris)} database connections to process.")

    for source_path in all_file_sources:
        process_source_file(source_path, db_connection, config)

    for db_uri in db_uris:
        try:
            if db_uri.startswith('sqlite:///'):
                db_path = db_uri.replace('sqlite:///', '')
                print(f"  -> Creating VIEWs for SQLite DB: {db_path}")
                db_connection.execute("INSTALL sqlite; LOAD sqlite;")
                tables = db_connection.execute(
                    f"SELECT name FROM sqlite_scan('{db_path}', 'sqlite_master') WHERE type='table';").fetchall()
                for (table_name,) in tables:
                    view_name = generate_table_name(db_path, table_name)
                    print(f"    -> Creating VIEW '{view_name}' for table '{table_name}'")
                    db_connection.execute(
                        f"""CREATE OR REPLACE VIEW "{view_name}" AS SELECT * FROM sqlite_scan('{db_path}', '{table_name}');""")
        except Exception as e:
            print(f"  ❌ Error processing database {db_uri}: {e}")

    db_connection.close()
    print(f"\n✅ Catalog build complete. File is at: {config['output_db_file']}")


def watch_folders(config):
    """Starts a service to watch for file changes and update the catalog in real-time."""
    print("✅ Starting file watcher... (Press Ctrl+C to exit)")
    print("Watcher will update the DuckDB catalog in real-time.")

    db_connection = get_db_connection(config)
    event_handler = DebouncedChangeHandler(config['debounce_seconds'], db_connection)
    observer = Observer()

    _, remote_uris, _ = [], [], []
    for source in config['sources']:
        if source.startswith(('http://', 'https://', 's3://')): remote_uris.append(source)

    watch_paths = {str(Path(s.split('**')[0].split('*')[0])) for s in config['sources'] if
                   not s.startswith(('http', 'sqlite', 's3'))}

    for path in watch_paths:
        if Path(path).is_dir():
            observer.schedule(event_handler, path, recursive=True)
            print(f"  -> Watching directory: {path}")

    observer.start()

    # Start the remote poller if enabled
    poller_stop_event = Event()
    poller_thread = None
    if config.get('polling_interval_seconds', 0) > 0:
        poller_thread = Thread(target=poll_remote_sources, args=(remote_uris, db_connection, config, poller_stop_event), daemon=True)
        poller_thread.start()

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        if poller_thread:
            poller_stop_event.set()

    observer.join()
    if poller_thread:
        poller_thread.join()

    db_connection.close()
    print("\nWatcher stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="duck-tape: Build or watch a DuckDB data catalog from various sources.")
    parser.add_argument('--config', default='config.yaml', help="Path to the YAML configuration file.")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--build', action='store_true', help="Perform a full, one-time build (default).")
    mode_group.add_argument('--watch', action='store_true', help="Watch source directories and update the catalog.")
    args = parser.parse_args()

    config = load_config(args.config)

    is_build_mode = not args.watch

    if is_build_mode:
        build_catalog(config)

    if args.watch:
        print("\nPerforming initial build before starting watcher...")
        build_catalog(config)
        watch_folders(config)


if __name__ == '__main__':
    # To run this script, you may need to install libraries:
    # pip install pandas duckdb pyyaml openpyxl lxml html5lib markdown pyarrow fastavro watchdog requests s3fs
    # For optional PDF support: pip install "camelot-py[cv]"
    main()

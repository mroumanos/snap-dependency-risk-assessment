import logging
import pandas as pd
from utils.db import get_conn
from sqlalchemy import text


def parse_nhgis_csv(file_path: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load NHGIS CSV where row 1 has column ids and row 2 has descriptions."""
    df = pd.read_csv(file_path, header=None, dtype=str)
    if len(df.index) < 2:
        logging.error(f"NHGIS file {file_path} is missing column description row")
        return pd.DataFrame(), {}

    column_ids = df.iloc[0].tolist()
    column_descriptions = df.iloc[1].tolist()
    data_df = df.iloc[2:].reset_index(drop=True)
    data_df.columns = column_ids

    description_map = {col: desc for col, desc in zip(column_ids, column_descriptions)}
    return data_df, description_map


def quote_ident(identifier: str) -> str:
    """Safely quote identifiers for SQL."""
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def apply_column_comments(conn, table_name: str, column_comments: dict[str, str]) -> None:
    """Apply COMMENT statements for each column using the provided descriptions."""
    quoted_table = quote_ident(table_name)
    for column, description in column_comments.items():
        if pd.isna(description):
            continue
        quoted_column = quote_ident(column)
        conn.execute(
            text(f"COMMENT ON COLUMN {quoted_table}.{quoted_column} IS :description"),
            {"description": str(description)}
        )


logger = logging.getLogger(__name__)

sources = {
    'acs_2023_county': {
        'file': [
            'static/nhgis/nhgis0001_ds267_20235_county.csv',
            'static/nhgis/nhgis0004_ds267_20235_county.csv'
        ]
    },
    'acs_2023_blkgrp': {
        'file': 'static/nhgis/nhgis0001_ds267_20235_blck_grp.csv'
    },
    'acs_2023_state': {
        'file': 'static/nhgis/nhgis0001_ds267_20235_state.csv'
    },
    'acs_2023_tract': {
        'file': 'static/nhgis/nhgis0001_ds267_20235_tract.csv'
    }
}


def merge_nhgis_files(file_paths: list[str]) -> tuple[pd.DataFrame, dict[str, str]]:
    """Parse and horizontally merge NHGIS CSVs on the first column (GISJOIN)."""
    combined_df: pd.DataFrame | None = None
    combined_comments: dict[str, str] = {}

    for idx, path in enumerate(file_paths):
        df, comments = parse_nhgis_csv(path)
        if df.empty:
            logger.warning(f"No data in NHGIS file {path}; skipping")
            continue

        join_col = df.columns[0]

        if combined_df is None:
            combined_df = df
            combined_comments.update(comments)
            continue

        if join_col not in combined_df.columns:
            logger.error(f"Join column {join_col} not found in existing data; skipping {path}")
            continue

        # Ensure unique column names before merging (except join column).
        rename_map = {}
        for col in df.columns[1:]:
            new_col = col
            while new_col in combined_df.columns:
                new_col = f"{new_col}_{idx}"
            rename_map[col] = new_col

        df = df.rename(columns=rename_map)
        comments = {rename_map.get(col, col): desc for col, desc in comments.items()}

        combined_df = combined_df.merge(df, how='left', on=join_col)
        for col, desc in comments.items():
            if col == join_col:
                # Preserve original join column comment
                continue
            combined_comments[col] = desc

    if combined_df is None:
        return pd.DataFrame(), {}

    return combined_df, combined_comments


async def run():
    """Run the full ETL process for SNAP retailers"""
    logger.info("Starting ETL process for remotely stored data...")
    
    for name, source in sources.items():
        logger.info(f"Fetching data for {name} from {source['file']}...")
        file_value = source['file']
        if isinstance(file_value, list):
            data_df, column_comments = merge_nhgis_files(file_value)
        else:
            data_df, column_comments = parse_nhgis_csv(file_value)
        if data_df.empty:
            logger.warning(f"No data found for {name}; skipping load")
            continue
        logger.info(f"Successfully fetched {len(data_df)} records for {name}")
        
        table_name = f"{name}_raw"
        logger.info(f"Loading {name} data into PostgreSQL PostGIS database as table {table_name}...")
        engine = get_conn()
        with engine.begin() as conn:
            data_df.to_sql(table_name, con=conn, if_exists='replace', index=False)
            apply_column_comments(conn, table_name, column_comments)
        logger.info(f"Successfully loaded {name} data into PostgreSQL PostGIS database")

    logger.info("ETL process completed successfully!")

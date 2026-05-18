"""Shared helpers for delimited text ingestion (CSV/TXT)."""

from __future__ import annotations

import csv
import logging
import re
from typing import Optional, Tuple


def detect_frequent_delimiter(
    sample_data: str,
    *,
    fallback: str,
    logger: Optional[logging.Logger] = None,
) -> str:
    """
    Detect the most frequent non-alphanumeric, non-space character in the sample data.
    """

    potential_delimiters = re.findall(r"[^\w\s]", sample_data)
    if not potential_delimiters:
        if logger:
            logger.warning("No potential delimiters found. Falling back to default '%s'.", fallback)
        return fallback

    delimiter_counts = {char: potential_delimiters.count(char) for char in set(potential_delimiters)}
    most_frequent_delimiter = max(delimiter_counts, key=delimiter_counts.get)
    if logger:
        logger.info("Detected most frequent delimiter: %s", most_frequent_delimiter)
    return most_frequent_delimiter


def infer_layout(
    sample_data: str,
    *,
    fallback_delimiter: str,
    assume_header: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Tuple[str, Optional[int], int]:
    """
    Infer delimiter, header line, and first data line from a delimited sample.

    Returns (delimiter, header_line_index, start_line_index).
    """

    sniffer = csv.Sniffer()
    delimiter = fallback_delimiter
    try:
        dialect = sniffer.sniff(sample_data)
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = detect_frequent_delimiter(sample_data, fallback=fallback_delimiter, logger=logger)

    try:
        has_header = sniffer.has_header(sample_data)
    except csv.Error:
        has_header = assume_header

    if has_header:
        header_line = 0
        start_line = 1
    else:
        header_line = None
        start_line = 0

    return delimiter, header_line, start_line


__all__ = ["detect_frequent_delimiter", "infer_layout"]


#!/usr/bin/env python3
"""
Enhanced Vim-like Terminal CSV Editor
Lightning fast, buffer-based, stream saves, curses-powered with QoL improvements
"""

import curses
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional, Set, Dict, Any
import polars as pl
import openpyxl
from io import StringIO
import threading
import time
from collections import defaultdict
import re
import json
import zstandard as zstd
import tempfile
import pandas as pd

class CCSVCompressor:
    def __init__(self, compression_level=3, memory_limit_mb=100):
        self.compressor = zstd.ZstdCompressor(level=compression_level)
        self.decompressor = zstd.ZstdDecompressor()
        self.memory_limit = memory_limit_mb * 1024 * 1024
    
    def should_use_streaming(self, file_path: str) -> bool:
        return Path(file_path).stat().st_size > self.memory_limit
    
    def decompress_ccsv_streaming(self, ccsv_file_path: str) -> Optional[str]:
        try:
            temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False, suffix='.csv')
            temp_path = temp_file.name
            temp_file.close()
            
            with open(ccsv_file_path, 'rb') as infile:
                with open(temp_path, 'wb') as outfile:
                    self.decompressor.copy_stream(infile, outfile)
            return temp_path
        except Exception:
            return None
    
    def decompress_ccsv_to_memory(self, ccsv_file_path: str) -> Optional[str]:
        try:
            if self.should_use_streaming(ccsv_file_path):
                return None
            with open(ccsv_file_path, 'rb') as infile:
                compressed_data = infile.read()
            decompressed_data = self.decompressor.decompress(compressed_data)
            return decompressed_data.decode('utf-8')
        except Exception:
            return None
    
    def compress_dataframe_to_ccsv(self, df: pl.DataFrame, ccsv_file_path: str) -> Tuple[bool, Dict[str, Any]]:
        """Compress Polars DataFrame directly to .ccsv format with metrics"""
        try:
            start_time = time.time()
            
            # Convert Polars DataFrame to CSV string
            csv_string = df.write_csv()
            csv_bytes = csv_string.encode('utf-8')
            original_size = len(csv_bytes)
            
            # Compress and save
            compressed_data = self.compressor.compress(csv_bytes)
            with open(ccsv_file_path, 'wb') as outfile:
                outfile.write(compressed_data)
            
            compression_time = time.time() - start_time
            compressed_size = len(compressed_data)
            
            metrics = {
                'original_size': original_size,
                'compressed_size': compressed_size,
                'compression_ratio': original_size / compressed_size,
                'space_saved_percent': ((original_size - compressed_size) / original_size) * 100,
                'compression_time': compression_time,
                'compressed_file': ccsv_file_path
            }
            
            return True, metrics
        except Exception:
            return False, {}


class UndoManager:
    """Undo/Redo functionality for CSV operations"""
    
    def __init__(self, max_history: int = 100):
        self.history: List[Dict] = []
        self.current_index = -1
        self.max_history = max_history
    
    def save_state(self, data: List[List[str]], headers: List[str], description: str = ""):
        """Save current state for undo"""
        # Remove any redo history when new action is performed
        self.history = self.history[:self.current_index + 1]
        
        state = {
            'data': [row.copy() for row in data],
            'headers': headers.copy(),
            'description': description,
            'timestamp': time.time()
        }
        
        self.history.append(state)
        self.current_index += 1
        
        # Limit history size
        if len(self.history) > self.max_history:
            self.history.pop(0)
            self.current_index -= 1
    
    def undo(self) -> Optional[Dict]:
        """Get previous state"""
        if self.current_index > 0:
            self.current_index -= 1
            return self.history[self.current_index]
        return None
    
    def redo(self) -> Optional[Dict]:
        """Get next state"""
        if self.current_index < len(self.history) - 1:
            self.current_index += 1
            return self.history[self.current_index]
        return None
    
    def can_undo(self) -> bool:
        return self.current_index > 0
    
    def can_redo(self) -> bool:
        return self.current_index < len(self.history) - 1


class SearchManager:
    """Search and replace functionality"""
    
    def __init__(self):
        self.last_search = ""
        self.case_sensitive = False
        self.regex_mode = False
        self.search_results: List[Tuple[int, int]] = []
        self.current_result_index = -1
    
    def search(self, data: List[List[str]], query: str, case_sensitive: bool = False, regex_mode: bool = False) -> List[Tuple[int, int]]:
        """Search for text in data"""
        self.last_search = query
        self.case_sensitive = case_sensitive
        self.regex_mode = regex_mode
        self.search_results = []
        self.current_result_index = -1
        
        if not query:
            return []
        
        search_text = query if case_sensitive else query.lower()
        
        for row_idx, row in enumerate(data):
            for col_idx, cell in enumerate(row):
                cell_text = cell if case_sensitive else cell.lower()
                
                if regex_mode:
                    try:
                        if re.search(query, cell, re.IGNORECASE if not case_sensitive else 0):
                            self.search_results.append((row_idx, col_idx))
                    except re.error:
                        pass  # Invalid regex, skip
                else:
                    if search_text in cell_text:
                        self.search_results.append((row_idx, col_idx))
        
        return self.search_results
    
    def next_result(self) -> Optional[Tuple[int, int]]:
        """Get next search result"""
        if not self.search_results:
            return None
        
        self.current_result_index = (self.current_result_index + 1) % len(self.search_results)
        return self.search_results[self.current_result_index]
    
    def prev_result(self) -> Optional[Tuple[int, int]]:
        """Get previous search result"""
        if not self.search_results:
            return None
        
        self.current_result_index = (self.current_result_index - 1) % len(self.search_results)
        return self.search_results[self.current_result_index]


class CSVBuffer:
    """Enhanced buffer for CSV data with streaming capabilities"""
    
    def __init__(self):
        self.data: List[List[str]] = []
        self.headers: List[str] = []
        self.dirty_rows: Set[int] = set()
        self.dirty = False
        self.file_path: Optional[str] = None
        self.original_types: Dict[int, type] = {}
        self.undo_manager = UndoManager()
        self.editor_ref = None  # Reference to parent editor for sheet selection
        
        # Enhanced features
        self.column_widths: Dict[int, int] = {}  # Custom column widths
        self.frozen_rows = 0
        self.frozen_cols = 0
        self.filters: Dict[int, str] = {}  # Column filters
        self.sort_column = -1
        self.sort_ascending = True
        self.current_sheet = None  # Track current Excel sheet name

        self.compressor = CCSVCompressor()
        self.file_format = 'csv'  # Track format: 'csv', 'xlsx', 'ccsv'
        self.last_load_time = 0.0
        self.last_save_time = 0.0
    
    def save_state(self, description: str = ""):
        """Save current state for undo"""
        self.undo_manager.save_state(self.data, self.headers, description)
    
    def undo(self) -> bool:
        """Undo last operation"""
        state = self.undo_manager.undo()
        if state:
            self.data = state['data']
            self.headers = state['headers']
            self.dirty = True
            return True
        return False
    
    def redo(self) -> bool:
        """Redo last undone operation"""
        state = self.undo_manager.redo()
        if state:
            self.data = state['data']
            self.headers = state['headers']
            self.dirty = True
            return True
        return False
    
    def auto_resize_columns(self):
        """Auto-resize columns based on content"""
        self.column_widths.clear()
        
        for col_idx in range(len(self.headers)):
            max_width = len(self.headers[col_idx]) + 2  # Header width + padding
            
            for row in self.data:
                if col_idx < len(row):
                    cell_width = len(str(row[col_idx])) + 2
                    max_width = max(max_width, cell_width)
            
            # Reasonable limits
            self.column_widths[col_idx] = min(max(max_width, 8), 50)
    
    def get_column_width(self, col_idx: int) -> int:
        """Get width for specific column"""
        return self.column_widths.get(col_idx, 12)
    
    def set_column_width(self, col_idx: int, width: int):
        """Set custom width for column"""
        self.column_widths[col_idx] = max(4, min(width, 100))
    
    def sort_by_column(self, col_idx: int, ascending: bool = True):
        """Sort data by column using polars for speed"""
        if not self.data or col_idx >= len(self.headers):
            return False
        
        self.save_state(f"Sort by {self.headers[col_idx]}")
        
        try:
            # Convert to polars for ultra-fast sorting
            df = self.to_dataframe()
            df_sorted = df.sort(self.headers[col_idx], descending=not ascending)
            
            # Convert back to buffer format
            self.data = []
            for row in df_sorted.iter_rows():
                row_data = []
                for val in row:
                    if val is None:
                        row_data.append("")
                    else:
                        row_data.append(str(val))
                self.data.append(row_data)
            
            self.sort_column = col_idx
            self.sort_ascending = ascending
            self.dirty = True
            return True
            
        except Exception:
            return False
    
    def apply_filter(self, col_idx: int, filter_text: str):
        """Apply filter to column (placeholder for future implementation)"""
        if filter_text:
            self.filters[col_idx] = filter_text
        elif col_idx in self.filters:
            del self.filters[col_idx]
    
    def load_from_file(self, file_path: str) -> bool:
        """Load data with polars for 3-5x faster performance and smart memory management"""
        try:
            start_time = time.time()
            path = Path(file_path)
            
            if not path.exists():
                self.headers = ["New_Col_1"]
                self.data = [[""] ]
                self.file_path = file_path
                self.file_format = 'csv'
                self.dirty = True
                self.dirty_rows.clear()
                self.original_types[0] = str
                self.save_state("Initial creation")
                self.stream_save()
                return True
            
            suffix = path.suffix.lower()
            file_size = path.stat().st_size
            temp_csv_path = None
            
            if suffix == '.ccsv':
                if self.compressor.should_use_streaming(file_path):
                    temp_csv_path = self.compressor.decompress_ccsv_streaming(file_path)
                    if temp_csv_path is None:
                        return False
                    df = pl.read_csv(temp_csv_path)
                    os.unlink(temp_csv_path)
                else:
                    csv_content = self.compressor.decompress_ccsv_to_memory(file_path)
                    if csv_content is None:
                        return False
                    from io import StringIO
                    csv_io = StringIO(csv_content)
                    df = pl.read_csv(csv_io)
                
                self.file_format = 'ccsv'
                
            elif suffix == '.csv':
                df = pl.read_csv(file_path)
                self.file_format = 'csv'
                
            elif suffix in ['.xlsx', '.xls']:
                file_size_mb = file_size / (1024 * 1024)
                if file_size_mb > 50 and self.editor_ref:
                    self.editor_ref.status_message = f"Loading Excel file ({file_size_mb:.1f}MB)..."
                
                sheet_name = self.select_excel_sheet(file_path)
                if sheet_name is None:
                    return False
                
                # Use pandas for Excel, then convert to polars
                pandas_df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                df = pl.from_pandas(pandas_df)
                
                self.file_format = 'xlsx'
                base_name = path.stem
                self.file_path = str(path.parent / f"{base_name}_{sheet_name}.csv")
                self.current_sheet = sheet_name
            else:
                return False
            
            # Handle empty files
            if df.height == 0:
                self.headers = ["New_Col_1"]
                self.data = [[""] ]
                self.dirty = True
                self.dirty_rows.clear()
                self.original_types[0] = str
                self.save_state("Empty file loaded")
                return True
            
            # Convert DataFrame to buffer format
            self.headers = df.columns
            self.data = []
            
            # Store original types for polars
            for i, col in enumerate(df.columns):
                dtype = df[col].dtype
                if dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
                    self.original_types[i] = int
                elif dtype in [pl.Float32, pl.Float64]:
                    self.original_types[i] = float
                else:
                    self.original_types[i] = str
            
            # Convert to string matrix
            for row in df.iter_rows():
                row_data = []
                for val in row:
                    if val is None:
                        row_data.append("")
                    else:
                        row_data.append(str(val))
                self.data.append(row_data)
            
            if not hasattr(self, 'file_path') or not self.file_path:
                self.file_path = file_path
            
            # Record performance
            load_time = time.time() - start_time
            self.last_load_time = load_time
            
            self.dirty = False
            self.dirty_rows.clear()
            self.save_state("File loaded")
            self.auto_resize_columns()

            # Show performance message with actual file sizes
            if self.editor_ref:
                actual_file_size = file_size
                size_mb = actual_file_size / (1024 * 1024)
                
                if self.file_format == 'ccsv':
                    # For CCSV, estimate what the uncompressed size would be
                    estimated_ratio = self.estimate_compression_ratio()
                    estimated_uncompressed_size = actual_file_size * estimated_ratio
                    estimated_uncompressed_mb = estimated_uncompressed_size / (1024 * 1024)
                    time_saved_estimate = (estimated_uncompressed_mb / size_mb - 1) * load_time
                    
                    self.editor_ref.status_message = (
                        f"⚡ CCSV loaded {size_mb:.1f}MB in {load_time:.1f}s "
                        f"(estimated {estimated_uncompressed_mb:.0f}MB uncompressed, saved ~{time_saved_estimate:.1f}s)"
                    )
                
                elif self.file_format == 'csv':
                    estimated_ratio = self.estimate_compression_ratio()
                    estimated_compressed_mb = size_mb / estimated_ratio
                    
                    if size_mb > 20:  # Only show compression suggestion for larger files
                        self.editor_ref.status_message = (
                            f"🚀 Polars loaded {size_mb:.1f}MB CSV in {load_time:.1f}s "
                            f"(compress to ~{estimated_compressed_mb:.0f}MB with :compress)"
                        )
                    else:
                        self.editor_ref.status_message = f"🚀 Polars loaded {size_mb:.1f}MB CSV in {load_time:.1f}s"
                
                else:
                    self.editor_ref.status_message = f"{self.file_format.upper()} loaded {size_mb:.1f}MB in {load_time:.1f}s"
            
            return True
            
        except Exception as e:
            if temp_csv_path and Path(temp_csv_path).exists():
                os.unlink(temp_csv_path)
            
            self.headers = ["New_Col_1"]
            self.data = [[""] ]
            self.file_path = file_path
            self.file_format = 'csv'
            self.dirty = True
            self.dirty_rows.clear()
            self.original_types[0] = str
            self.save_state("Error recovery")
            return True
    
    def select_excel_sheet(self, file_path: str) -> Optional[str]:
        """Show sheet selection dialog for Excel files"""
        try:
            # Get list of available sheets
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            if len(sheet_names) == 1:
                # Only one sheet, use it directly
                return sheet_names[0]
            
            # Use editor's sheet selector if available
            if self.editor_ref and hasattr(self.editor_ref, 'show_sheet_selector'):
                return self.editor_ref.show_sheet_selector(sheet_names, file_path)
            else:
                # Fallback to first sheet
                return sheet_names[0]
            
        except Exception:
            return None
        
    def save_as_ccsv(self, file_path: str = None) -> bool:
        """Save as compressed .ccsv format with actual performance metrics"""
        try:
            save_path = file_path or self.file_path
            if not save_path:
                return False
            
            if not save_path.endswith('.ccsv'):
                save_path = save_path.rsplit('.', 1)[0] + '.ccsv'
            
            # Get actual uncompressed size
            df = self.to_dataframe()
            csv_string = df.write_csv()
            actual_uncompressed_size = len(csv_string.encode('utf-8'))
            
            # Perform compression and get real metrics
            success, metrics = self.compressor.compress_dataframe_to_ccsv(df, save_path)
            
            if success:
                # Get actual compressed file size from disk
                actual_compressed_size = Path(save_path).stat().st_size
                
                # Calculate real metrics
                actual_ratio = actual_uncompressed_size / actual_compressed_size
                actual_savings_mb = (actual_uncompressed_size - actual_compressed_size) / (1024 * 1024)
                actual_savings_percent = ((actual_uncompressed_size - actual_compressed_size) / actual_uncompressed_size) * 100
                
                self.file_path = save_path
                self.file_format = 'ccsv'
                self.dirty = False
                self.dirty_rows.clear()
                self.last_save_time = metrics['compression_time']
                
                # Store actual compression metrics for future reference
                self.actual_compression_ratio = actual_ratio
                self.actual_uncompressed_size = actual_uncompressed_size
                self.actual_compressed_size = actual_compressed_size
                
                # Show real metrics to user
                if self.editor_ref:
                    save_time = metrics['compression_time']
                    uncompressed_mb = actual_uncompressed_size / (1024 * 1024)
                    compressed_mb = actual_compressed_size / (1024 * 1024)
                    
                    self.editor_ref.status_message = (
                        f"⚡ Compressed! {uncompressed_mb:.1f}MB → {compressed_mb:.1f}MB "
                        f"({actual_ratio:.1f}x smaller, {actual_savings_percent:.1f}% saved) in {save_time:.2f}s"
                    )
                
                return True
            return False
        except Exception:
            return False

    def to_dataframe(self) -> pl.DataFrame:
        """Convert buffer to Polars DataFrame with proper types"""
        df_data = {}
        for i, header in enumerate(self.headers):
            col_data = []
            for row in self.data:
                cell_value = row[i] if i < len(row) else ""
                
                # Convert based on original type
                if i in self.original_types and cell_value.strip():
                    try:
                        if self.original_types[i] == int:
                            col_data.append(int(cell_value))
                        elif self.original_types[i] == float:
                            col_data.append(float(cell_value))
                        else:
                            col_data.append(cell_value)
                    except ValueError:
                        col_data.append(cell_value)
                else:
                    col_data.append(cell_value if cell_value.strip() else None)
            
            df_data[header] = col_data
        
        return pl.DataFrame(df_data)
    
    def estimate_compression_ratio(self, sample_size: int = 1000) -> float:
        """Estimate compression ratio from actual current data sample"""
        try:
            if not self.data or not self.headers:
                return 2.5  # Conservative fallback
            
            # Take a representative sample
            sample_rows = min(sample_size, len(self.data))
            sample_data = self.data[:sample_rows]
            
            # Create sample DataFrame with actual data
            sample_df_data = {}
            for i, header in enumerate(self.headers):
                col_data = []
                for row in sample_data:
                    cell_value = row[i] if i < len(row) else ""
                    col_data.append(cell_value)
                sample_df_data[header] = col_data
            
            sample_df = pl.DataFrame(sample_df_data)
            
            # Get actual CSV string size
            csv_string = sample_df.write_csv()
            uncompressed_size = len(csv_string.encode('utf-8'))
            
            # Get actual compressed size
            compressed_data = self.compressor.compressor.compress(csv_string.encode('utf-8'))
            compressed_size = len(compressed_data)
            
            # Return real compression ratio
            if compressed_size > 0:
                ratio = uncompressed_size / compressed_size
                # Ensure reasonable bounds (compression ratios typically 1.5x to 10x)
                return max(1.5, min(ratio, 10.0))
            else:
                return 2.5
            
        except Exception:
            return 2.5  # Conservative fallback

    def get_compression_info(self) -> str:
        """Get performance information with actual file sizes and real compression data"""
        if not self.file_path:
            return ""
        
        path = Path(self.file_path)
        if not path.exists():
            return ""
        
        actual_file_size = path.stat().st_size
        size_mb = actual_file_size / (1024 * 1024)
        
        if self.file_format == 'ccsv':
            # Show actual CCSV metrics if available
            if hasattr(self, 'actual_compression_ratio') and hasattr(self, 'actual_uncompressed_size'):
                original_mb = self.actual_uncompressed_size / (1024 * 1024)
                savings_percent = ((self.actual_uncompressed_size - actual_file_size) / self.actual_uncompressed_size) * 100
                return f" [⚡ CCSV: {size_mb:.1f}MB (was {original_mb:.1f}MB, {savings_percent:.0f}% saved)]"
            else:
                # Fallback if no compression data available
                return f" [⚡ CCSV: {size_mb:.1f}MB compressed]"
        
        elif size_mb > 10:
            # Calculate potential compression based on actual current data
            potential_ratio = self.estimate_compression_ratio()
            potential_size_mb = size_mb / potential_ratio
            potential_savings_mb = size_mb - potential_size_mb
            potential_savings_percent = (potential_savings_mb / size_mb) * 100
            
            if size_mb > 50:
                return f" [💡 {size_mb:.0f}MB → ~{potential_size_mb:.0f}MB with :compress ({potential_savings_percent:.0f}% saved)]"
            else:
                return f" [{size_mb:.1f}MB → ~{potential_size_mb:.1f}MB with :compress]"
        
        return f" [{size_mb:.1f}MB]"
    
    def get_cell(self, row: int, col: int) -> str:
        """Get cell value safely"""
        if 0 <= row < len(self.data) and 0 <= col < len(self.data[0]) if self.data else False:
            return self.data[row][col]
        return ""
    
    def set_cell(self, row: int, col: int, value: str, save_state: bool = True) -> bool:
        """Set cell value with type preservation"""
        if not (0 <= row < len(self.data) and 0 <= col < len(self.data[0]) if self.data else False):
            return False
        
        old_value = self.data[row][col]
        if old_value == value:
            return True  # No change
        
        if save_state:
            self.save_state(f"Edit cell ({row+1},{col+1})")
        
        # Convert value based on original type
        if col in self.original_types:
            try:
                if self.original_types[col] == int and value.strip():
                    # Validate it's a valid integer
                    int(value)
                elif self.original_types[col] == float and value.strip():
                    # Validate it's a valid float
                    float(value)
            except ValueError:
                # If conversion fails, it stays as string
                pass
        
        self.data[row][col] = value
        self.dirty_rows.add(row)
        self.dirty = True
        return True
    
    def insert_row(self, position: int, save_state: bool = True) -> bool:
        """Insert empty row at position"""
        if not (0 <= position <= len(self.data)):
            return False
        
        if save_state:
            self.save_state(f"Insert row at {position+1}")
        
        empty_row = [""] * len(self.headers) if self.headers else []
        self.data.insert(position, empty_row)
        
        # Update dirty rows indices
        new_dirty = set()
        for r in self.dirty_rows:
            if r >= position:
                new_dirty.add(r + 1)
            else:
                new_dirty.add(r)
        self.dirty_rows = new_dirty
        self.dirty_rows.add(position)
        self.dirty = True
        return True
    
    def delete_row(self, position: int, save_state: bool = True) -> bool:
        """Delete row at position"""
        if not (0 <= position < len(self.data)) or len(self.data) <= 1:
            return False
        
        if save_state:
            self.save_state(f"Delete row {position+1}")
        
        del self.data[position]
        
        # Update dirty rows indices
        new_dirty = set()
        for r in self.dirty_rows:
            if r > position:
                new_dirty.add(r - 1)
            elif r < position:
                new_dirty.add(r)
            # r == position is removed
        self.dirty_rows = new_dirty
        self.dirty = True
        return True
    
    def insert_column(self, position: int, name: str = None, save_state: bool = True) -> bool:
        """Insert empty column at position"""
        if not (0 <= position <= len(self.headers)):
            return False
        
        if save_state:
            self.save_state(f"Insert column at {position+1}")
        
        col_name = name or f"Col_{len(self.headers)}"
        self.headers.insert(position, col_name)
        
        for row in self.data:
            row.insert(position, "")
        
        # Update original types
        new_types = {}
        for col, dtype in self.original_types.items():
            if col >= position:
                new_types[col + 1] = dtype
            else:
                new_types[col] = dtype
        new_types[position] = str
        self.original_types = new_types
        
        # Update column widths
        new_widths = {}
        for col, width in self.column_widths.items():
            if col >= position:
                new_widths[col + 1] = width
            else:
                new_widths[col] = width
        new_widths[position] = 12  # Default width
        self.column_widths = new_widths
        
        self.dirty = True
        return True
    
    def delete_column(self, position: int, save_state: bool = True) -> bool:
        """Delete column at position"""
        if not (0 <= position < len(self.headers)) or len(self.headers) <= 1:
            return False
        
        if save_state:
            self.save_state(f"Delete column {self.headers[position]}")
        
        del self.headers[position]
        for row in self.data:
            if position < len(row):
                del row[position]
        
        # Update original types
        new_types = {}
        for col, dtype in self.original_types.items():
            if col > position:
                new_types[col - 1] = dtype
            elif col < position:
                new_types[col] = dtype
            # col == position is removed
        self.original_types = new_types
        
        # Update column widths
        new_widths = {}
        for col, width in self.column_widths.items():
            if col > position:
                new_widths[col - 1] = width
            elif col < position:
                new_widths[col] = width
        self.column_widths = new_widths
        
        self.dirty = True
        return True
    
    def duplicate_row(self, position: int, save_state: bool = True) -> bool:
        """Duplicate row at position"""
        if not (0 <= position < len(self.data)):
            return False
        
        if save_state:
            self.save_state(f"Duplicate row {position+1}")
        
        new_row = self.data[position].copy()
        self.data.insert(position + 1, new_row)
        
        # Update dirty rows indices
        new_dirty = set()
        for r in self.dirty_rows:
            if r > position:
                new_dirty.add(r + 1)
            else:
                new_dirty.add(r)
        self.dirty_rows = new_dirty
        self.dirty_rows.add(position + 1)
        self.dirty = True
        return True
    
    def stream_save(self, file_path: str = None) -> bool:
        """Enhanced save with actual file size reporting"""
        try:
            start_time = time.time()
            save_path = file_path or self.file_path
            if not save_path:
                return False
            
            # If originally .ccsv, save as .ccsv
            if self.file_format == 'ccsv' and not file_path:
                return self.save_as_ccsv(save_path)
            
            # Otherwise save as CSV
            if not save_path.endswith('.csv'):
                save_path = save_path.rsplit('.', 1)[0] + '.csv'
            
            df = self.to_dataframe()
            df.write_csv(save_path)
            
            save_time = time.time() - start_time
            self.last_save_time = save_time
            
            # Get actual file size from disk
            actual_file_size = Path(save_path).stat().st_size
            actual_size_mb = actual_file_size / (1024 * 1024)
            
            self.dirty = False
            self.dirty_rows.clear()
            
            # Show actual save performance
            if self.editor_ref:
                speed_mbps = actual_size_mb / save_time if save_time > 0 else 0
                
                # Also show compression potential
                estimated_ratio = self.estimate_compression_ratio()
                estimated_compressed_mb = actual_size_mb / estimated_ratio
                potential_savings = actual_size_mb - estimated_compressed_mb
                
                if actual_size_mb > 10:
                    self.editor_ref.status_message = (
                        f"🚀 Saved {actual_size_mb:.1f}MB in {save_time:.2f}s ({speed_mbps:.1f} MB/s) "
                        f"- compress to save {potential_savings:.1f}MB"
                    )
                else:
                    self.editor_ref.status_message = f"🚀 Saved {actual_size_mb:.1f}MB in {save_time:.2f}s ({speed_mbps:.1f} MB/s)"
            
            return True
            
        except Exception:
            return False


class VimCSVEditor:
    """Enhanced Vim-like CSV editor with curses interface"""
    
    def __init__(self):
        self.buffer = CSVBuffer()
        self.buffer.editor_ref = self  # Set reference for sheet selection
        self.cursor_row = 0
        self.cursor_col = 0
        self.scroll_row = 0
        self.scroll_col = 0
        self.mode = 'NORMAL'  # NORMAL, INSERT, VISUAL, COMMAND, SEARCH
        self.selected_cells: Set[Tuple[int, int]] = set()
        self.clipboard: List[List[str]] = []
        self.status_message = "Ready"
        self.command_buffer = ""
        self.edit_buffer = ""
        self.visual_start: Optional[Tuple[int, int]] = None
        self.search_manager = SearchManager()
        
        # Display settings
        self.visible_rows = 20
        self.visible_cols = 8
        self.col_width = 12
        self.show_grid_lines = True
        self.show_row_numbers = True
        self.show_column_letters = True
        
        # Auto-save thread
        self.auto_save_enabled = True
        self.auto_save_interval = 30  # seconds
        self.last_save_time = time.time()
        
        # Enhanced features
        self.repeat_count = 1  # For vim-like repeat commands
        self.last_command = ""
        self.macro_recording = False
        self.macro_buffer = ""
        
        # Multi-cursor support (basic)
        self.additional_cursors: List[Tuple[int, int]] = []
    
    def start_auto_save(self):
        """Start auto-save thread"""
        def auto_save_worker():
            while self.auto_save_enabled:
                time.sleep(5)  # Check every 5 seconds
                if (self.buffer.dirty and 
                    time.time() - self.last_save_time > self.auto_save_interval):
                    if self.buffer.file_path:
                        if self.buffer.stream_save():
                            self.status_message = "Auto-saved"
                            self.last_save_time = time.time()
        
        auto_save_thread = threading.Thread(target=auto_save_worker, daemon=True)
        auto_save_thread.start()
    
    def parse_count_and_command(self, key: int) -> Tuple[int, int]:
        """Parse repeat count for vim-like commands"""
        if ord('1') <= key <= ord('9'):
            self.repeat_count = self.repeat_count * 10 + (key - ord('0'))
            return self.repeat_count, -1  # -1 means continue collecting count
        else:
            count = self.repeat_count
            self.repeat_count = 1  # Reset for next command
            return count, key
    
    def adjust_scroll(self):
        """Enhanced scrolling with smooth behavior"""
        if not self.buffer.data:
            return
        
        rows = len(self.buffer.data)
        cols = len(self.buffer.headers)
        
        # Vertical scrolling - keep cursor in view with margin
        margin = 3
        if self.cursor_row < self.scroll_row + margin:
            self.scroll_row = max(0, self.cursor_row - margin)
        elif self.cursor_row >= self.scroll_row + self.visible_rows - margin:
            self.scroll_row = min(max(0, rows - self.visible_rows), 
                                self.cursor_row - self.visible_rows + margin + 1)
        
        # Horizontal scrolling with dynamic column widths
        total_width = 0
        visible_cols = 0
        
        # Calculate how many columns fit in view
        for i in range(self.scroll_col, len(self.buffer.headers)):
            col_width = self.buffer.get_column_width(i)
            if total_width + col_width > self.visible_cols * 12:  # Approximate screen width
                break
            total_width += col_width
            visible_cols += 1
        
        # Adjust horizontal scroll
        if self.cursor_col < self.scroll_col:
            self.scroll_col = self.cursor_col
        elif self.cursor_col >= self.scroll_col + visible_cols:
            self.scroll_col = max(0, self.cursor_col - visible_cols + 1)
        
        # Bounds
        self.scroll_row = max(0, min(self.scroll_row, max(0, rows - self.visible_rows)))
        self.scroll_col = max(0, min(self.scroll_col, max(0, cols - self.visible_cols)))
    
    def move_cursor(self, delta_row: int, delta_col: int, count: int = 1):
        """Move cursor with vim-like bounds and repeat support"""
        if not self.buffer.data:
            return
        
        rows = len(self.buffer.data)
        cols = len(self.buffer.headers)
        
        # Apply count multiplier
        delta_row *= count
        delta_col *= count
        
        self.cursor_row = max(0, min(rows - 1, self.cursor_row + delta_row))
        self.cursor_col = max(0, min(cols - 1, self.cursor_col + delta_col))
        
        self.adjust_scroll()
    
    def enter_search_mode(self, forward: bool = True):
        """Enter search mode"""
        self.mode = 'SEARCH'
        self.command_buffer = ""
        self.status_message = "/" if forward else "?"
    
    def perform_search(self, query: str, forward: bool = True):
        """Perform search and navigate to first result"""
        results = self.search_manager.search(self.buffer.data, query)
        
        if results:
            if forward:
                next_pos = self.search_manager.next_result()
            else:
                next_pos = self.search_manager.prev_result()
            
            if next_pos:
                self.cursor_row, self.cursor_col = next_pos
                self.adjust_scroll()
                self.status_message = f"Found {len(results)} matches"
            else:
                self.status_message = "No matches found"
        else:
            self.status_message = "Pattern not found"
    
    def toggle_column_width_mode(self):
        """Toggle between auto-resize and fixed width"""
        if hasattr(self, 'auto_resize_mode'):
            self.auto_resize_mode = not self.auto_resize_mode
        else:
            self.auto_resize_mode = True
            
        if self.auto_resize_mode:
            self.buffer.auto_resize_columns()
            self.status_message = "Auto-resize columns enabled"
        else:
            self.status_message = "Fixed column width mode"
    
    def enter_insert_mode(self):
        """Enter insert mode for current cell"""
        self.mode = 'INSERT'
        self.edit_buffer = self.buffer.get_cell(self.cursor_row, self.cursor_col)
        self.status_message = "-- INSERT --"
    
    def enter_fullscreen_edit_mode(self):
        """Enter fullscreen scrollable edit mode for large text"""
        self.mode = 'FULLSCREEN_EDIT'
        self.edit_buffer = self.buffer.get_cell(self.cursor_row, self.cursor_col)
        self.edit_cursor_line = 0
        self.edit_cursor_col = 0
        self.edit_scroll_line = 0
        self.edit_scroll_col = 0  # Add horizontal scroll
        self.edit_lines = self.edit_buffer.split('\n') if self.edit_buffer else ['']
        self.status_message = "-- FULLSCREEN EDIT -- (Ctrl+X to save & exit, Esc to cancel)"
    
    def exit_insert_mode(self, save: bool = True):
        """Exit insert mode"""
        if save and self.mode == 'INSERT':
            self.buffer.set_cell(self.cursor_row, self.cursor_col, self.edit_buffer)
        
        self.mode = 'NORMAL'
        self.edit_buffer = ""
        self.status_message = "Ready"
    
    def exit_fullscreen_edit_mode(self, save: bool = True):
        """Exit fullscreen edit mode"""
        if save and self.mode == 'FULLSCREEN_EDIT':
            # Join lines back into single text
            text_content = '\n'.join(self.edit_lines)
            self.buffer.set_cell(self.cursor_row, self.cursor_col, text_content)
        
        self.mode = 'NORMAL'
        self.edit_buffer = ""
        self.edit_lines = []
        self.status_message = "Ready"
    
    def enter_visual_mode(self):
        """Enter visual mode"""
        self.mode = 'VISUAL'
        self.visual_start = (self.cursor_row, self.cursor_col)
        self.selected_cells = {(self.cursor_row, self.cursor_col)}
        self.status_message = "-- VISUAL --"
    
    def update_visual_selection(self):
        """Update visual selection"""
        if self.mode != 'VISUAL' or not self.visual_start:
            return
        
        start_row, start_col = self.visual_start
        end_row, end_col = self.cursor_row, self.cursor_col
        
        # Ensure proper ordering
        min_row, max_row = sorted([start_row, end_row])
        min_col, max_col = sorted([start_col, end_col])
        
        # Update selection
        self.selected_cells.clear()
        for r in range(min_row, max_row + 1):
            for c in range(min_col, max_col + 1):
                self.selected_cells.add((r, c))
    
    def exit_visual_mode(self):
        """Exit visual mode"""
        self.mode = 'NORMAL'
        self.visual_start = None
        self.selected_cells.clear()
        self.status_message = "Ready"
    
    def yank_selection(self):
        """Yank (copy) selection to clipboard"""
        if not self.selected_cells:
            # Yank current cell
            self.clipboard = [[self.buffer.get_cell(self.cursor_row, self.cursor_col)]]
        else:
            # Yank selected range
            rows = sorted(set(r for r, c in self.selected_cells))
            cols = sorted(set(c for r, c in self.selected_cells))
            
            self.clipboard = []
            for r in rows:
                row_data = []
                for c in cols:
                    if (r, c) in self.selected_cells:
                        row_data.append(self.buffer.get_cell(r, c))
                    else:
                        row_data.append("")
                self.clipboard.append(row_data)
        
        self.status_message = f"Yanked {len(self.clipboard)}x{len(self.clipboard[0]) if self.clipboard else 0}"
    
    def paste_clipboard(self):
        """Paste clipboard at cursor"""
        if not self.clipboard:
            return
        
        self.buffer.save_state("Paste operation")
        
        for r_offset, row_data in enumerate(self.clipboard):
            for c_offset, value in enumerate(row_data):
                target_row = self.cursor_row + r_offset
                target_col = self.cursor_col + c_offset
                if (target_row < len(self.buffer.data) and 
                    target_col < len(self.buffer.headers)):
                    self.buffer.set_cell(target_row, target_col, value, save_state=False)
        
        self.status_message = f"Pasted {len(self.clipboard)}x{len(self.clipboard[0])}"
    
    def handle_normal_mode(self, key: int) -> bool:
        """Enhanced normal mode with more vim-like features"""
        count, actual_key = self.parse_count_and_command(key)
        
        if actual_key == -1:  # Still collecting count
            self.status_message = f"Count: {count}"
            return True
        
        key = actual_key
        
        if key == ord('q'):
            return False  # Quit
        elif key == ord('I'):  # Capital I for fullscreen edit mode
            self.enter_fullscreen_edit_mode()
        elif key == ord('i'):
            self.enter_insert_mode()
        elif key == ord('a'):  # Insert after (Excel-like behavior)
            self.enter_insert_mode()
        elif key == ord('A'):  # Insert at end of cell / Goto cell
            self.goto_cell()
        elif key == ord('v'):
            self.enter_visual_mode()
        elif key == ord('V'):  # Visual line mode (select entire row)
            self.enter_visual_mode()
            self.select_entire_row()
        elif key == ord('y'):
            self.yank_selection()
            if self.mode == 'VISUAL':
                self.exit_visual_mode()
        elif key == ord('p'):
            self.paste_clipboard()
        elif key == ord('P'):  # Paste above/before
            self.paste_clipboard()  # Same as 'p' for now
        elif key == ord('u'):
            if self.buffer.undo():
                self.status_message = "Undo successful"
                self.adjust_scroll()
            else:
                self.status_message = "Nothing to undo"
        elif key == 18:  # Ctrl+R
            if self.buffer.redo():
                self.status_message = "Redo successful"
                self.adjust_scroll()
            else:
                self.status_message = "Nothing to redo"
        elif key == ord('o'):
            # Insert row below and enter insert mode
            for _ in range(count):
                self.buffer.insert_row(self.cursor_row + 1)
                self.move_cursor(1, 0)
            self.enter_insert_mode()
        elif key == ord('O'):
            # Insert row above and enter insert mode
            for _ in range(count):
                self.buffer.insert_row(self.cursor_row)
            self.enter_insert_mode()
        elif key == ord('d'):
            # Delete operations
            if self.mode == 'VISUAL':
                # Delete selected area
                self.buffer.save_state("Delete selection")
                for row, col in sorted(self.selected_cells, reverse=True):
                    self.buffer.set_cell(row, col, "", save_state=False)
                self.exit_visual_mode()
                self.status_message = "Deleted selection"
            else:
                # Delete current row (dd-like behavior)
                for _ in range(count):
                    if len(self.buffer.data) > 1:
                        self.buffer.delete_row(self.cursor_row)
                        if self.cursor_row >= len(self.buffer.data):
                            self.cursor_row = len(self.buffer.data) - 1
                self.adjust_scroll()
                # Force complete screen refresh to prevent multiline spillover
                self.stdscr.clear()
                self.status_message = f"Deleted {count} row(s)"
        elif key == ord('D'):
            # Duplicate current row
            for _ in range(count):
                self.buffer.duplicate_row(self.cursor_row)
            self.status_message = f"Duplicated row {count} time(s)"
        elif key == ord('x'):
            # Delete current cell content
            self.buffer.set_cell(self.cursor_row, self.cursor_col, "")
            self.status_message = "Cleared cell"
        elif key == ord('X'):
            # Delete current column
            if len(self.buffer.headers) > 1:
                self.buffer.delete_column(self.cursor_col)
                if self.cursor_col >= len(self.buffer.headers):
                    self.cursor_col = len(self.buffer.headers) - 1
                self.adjust_scroll()
                # Force complete screen refresh to prevent multiline spillover
                self.stdscr.clear()
                self.status_message = "Deleted column"
            else:
                self.status_message = "Cannot delete last column"
        elif key == ord('c'):
            # Insert column to the right of cursor
            self.buffer.insert_column(self.cursor_col + 1, f"NewCol_{len(self.buffer.headers)}")
            self.status_message = "Inserted column to right"
        elif key == ord('C'):
            # Insert column to the left of cursor
            self.buffer.insert_column(self.cursor_col, f"NewCol_{len(self.buffer.headers)}")
            self.status_message = "Inserted column to left"
        elif key == ord(':'):
            self.mode = 'COMMAND'
            self.command_buffer = ""
            self.status_message = ":"
        elif key == ord('/'):
            self.enter_search_mode(forward=True)
        elif key == ord('?'):
            self.enter_search_mode(forward=False)
        elif key == ord('n'):
            # Next search result
            next_pos = self.search_manager.next_result()
            if next_pos:
                self.cursor_row, self.cursor_col = next_pos
                self.adjust_scroll()
                self.status_message = f"Match {self.search_manager.current_result_index + 1}/{len(self.search_manager.search_results)}"
            else:
                self.status_message = "No search results"
        elif key == ord('N'):
            # Previous search result
            prev_pos = self.search_manager.prev_result()
            if prev_pos:
                self.cursor_row, self.cursor_col = prev_pos
                self.adjust_scroll()
                self.status_message = f"Match {self.search_manager.current_result_index + 1}/{len(self.search_manager.search_results)}"
            else:
                self.status_message = "No search results"
        elif key == ord('r'):
            # Replace character (enter single char replace mode)
            self.status_message = "Replace with: "
            replace_key = self.stdscr.getch()
            if 32 <= replace_key <= 126:
                old_value = self.buffer.get_cell(self.cursor_row, self.cursor_col)
                if old_value:
                    new_value = chr(replace_key) + old_value[1:]
                else:
                    new_value = chr(replace_key)
                self.buffer.set_cell(self.cursor_row, self.cursor_col, new_value)
                self.status_message = "Replaced character"
            else:
                self.status_message = "Replace cancelled"
        elif key == ord('R'):
            # Replace mode (overwrite cell content)
            self.mode = 'INSERT'
            self.edit_buffer = ""  # Start with empty buffer for replace mode
            self.status_message = "-- REPLACE --"
        elif key == ord('s'):
            # Sort by current column
            ascending = True
            if hasattr(self, 'last_sort_col') and self.last_sort_col == self.cursor_col:
                ascending = not getattr(self, 'last_sort_ascending', True)
            
            if self.buffer.sort_by_column(self.cursor_col, ascending):
                self.last_sort_col = self.cursor_col
                self.last_sort_ascending = ascending
                direction = "ascending" if ascending else "descending"
                self.status_message = f"Sorted by {self.buffer.headers[self.cursor_col]} ({direction})"
            else:
                self.status_message = "Sort failed"
        elif key == ord('S'):
            # Sort by current column (opposite direction)
            ascending = False
            if hasattr(self, 'last_sort_col') and self.last_sort_col == self.cursor_col:
                ascending = not getattr(self, 'last_sort_ascending', False)
            
            if self.buffer.sort_by_column(self.cursor_col, ascending):
                self.last_sort_col = self.cursor_col
                self.last_sort_ascending = ascending
                direction = "ascending" if ascending else "descending"
                self.status_message = f"Sorted by {self.buffer.headers[self.cursor_col]} ({direction})"
            else:
                self.status_message = "Sort failed"
        elif key == ord('='):
            # Auto-resize columns
            self.buffer.auto_resize_columns()
            self.status_message = "Auto-resized columns"
        elif key == ord('+'):
            # Increase column width
            old_width = self.buffer.get_column_width(self.cursor_col)
            self.buffer.set_column_width(self.cursor_col, old_width + 2)
            self.status_message = f"Column width: {self.buffer.get_column_width(self.cursor_col)}"
        elif key == ord('-'):
            # Decrease column width
            old_width = self.buffer.get_column_width(self.cursor_col)
            self.buffer.set_column_width(self.cursor_col, old_width - 2)
            self.status_message = f"Column width: {self.buffer.get_column_width(self.cursor_col)}"
        elif key == curses.KEY_F1 or key == ord('H'):
            self.show_help()
        # Enhanced navigation
        elif key == ord('h') or key == curses.KEY_LEFT:
            self.move_cursor(0, -1, count)
        elif key == ord('j') or key == curses.KEY_DOWN:
            self.move_cursor(1, 0, count)
        elif key == ord('k') or key == curses.KEY_UP:
            self.move_cursor(-1, 0, count)
        elif key == ord('l') or key == curses.KEY_RIGHT:
            self.move_cursor(0, 1, count)
        elif key == ord('w'):  # Word right (next column)
            self.move_cursor(0, 1, count)
        elif key == ord('b'):  # Word left (prev column)
            self.move_cursor(0, -1, count)
        elif key == ord('e'):  # Edit column name
            self.edit_column_name()
        elif key == ord('G'):  # Go to end
            if count > 1:
                # Go to specific line
                target_row = min(count - 1, len(self.buffer.data) - 1)
                self.cursor_row = target_row
            else:
                # Go to last row
                if self.buffer.data:
                    self.cursor_row = len(self.buffer.data) - 1
            self.adjust_scroll()
        elif key == ord('g'):  # Go to beginning (gg when repeated)
            self.cursor_row = 0
            self.adjust_scroll()
        elif key == ord('$'):  # End of row
            if self.buffer.headers:
                self.cursor_col = len(self.buffer.headers) - 1
                self.adjust_scroll()
        elif key == ord('0'):  # Beginning of row
            self.cursor_col = 0
            self.adjust_scroll()
        elif key == ord('^'):  # First non-empty column
            self.cursor_col = 0
            # Find first non-empty cell in current row
            for col in range(len(self.buffer.headers)):
                if self.buffer.get_cell(self.cursor_row, col).strip():
                    self.cursor_col = col
                    break
            self.adjust_scroll()
        # Page navigation
        elif key == 6:  # Ctrl+F
            self.move_cursor(self.visible_rows, 0, count)
        elif key == 2:  # Ctrl+B
            self.move_cursor(-self.visible_rows, 0, count)
        elif key == 4:  # Ctrl+D
            self.move_cursor(self.visible_rows // 2, 0, count)
        elif key == 21:  # Ctrl+U
            self.move_cursor(-self.visible_rows // 2, 0, count)
        elif key == ord('H'):  # Move to top of screen
            self.cursor_row = self.scroll_row
            self.adjust_scroll()
        elif key == ord('M'):  # Move to middle of screen
            self.cursor_row = self.scroll_row + self.visible_rows // 2
            self.cursor_row = min(self.cursor_row, len(self.buffer.data) - 1)
            self.adjust_scroll()
        elif key == ord('L'):  # Move to bottom of screen
            self.cursor_row = min(self.scroll_row + self.visible_rows - 1, len(self.buffer.data) - 1)
            self.adjust_scroll()
        
        return True

    def select_entire_row(self):
        """Select entire current row in visual mode"""
        if self.mode == 'VISUAL':
            self.selected_cells.clear()
            for col in range(len(self.buffer.headers)):
                self.selected_cells.add((self.cursor_row, col))
            self.status_message = f"Selected row {self.cursor_row + 1}"
    
    def handle_visual_mode(self, key: int) -> bool:
        """Enhanced visual mode key presses"""
        if key == 27:  # ESC
            self.exit_visual_mode()
        elif key == ord('y'):
            self.yank_selection()
            self.exit_visual_mode()
        elif key == ord('d'):
            # Delete selection
            self.buffer.save_state("Delete selection")
            for row, col in self.selected_cells:
                self.buffer.set_cell(row, col, "", save_state=False)
            self.exit_visual_mode()
            self.status_message = "Deleted selection"
        elif key == ord('c'):
            # Change selection (delete and enter insert mode)
            self.buffer.save_state("Change selection")
            for row, col in self.selected_cells:
                self.buffer.set_cell(row, col, "", save_state=False)
            # Move cursor to start of selection
            if self.selected_cells:
                min_row = min(r for r, c in self.selected_cells)
                min_col = min(c for r, c in self.selected_cells if r == min_row)
                self.cursor_row, self.cursor_col = min_row, min_col
            self.exit_visual_mode()
            self.enter_insert_mode()
        elif key == ord('a'):
            self.select_all_data()
        elif key == ord('r'):
            # Replace all selected cells with single character
            self.status_message = "Replace with: "
            replace_key = self.stdscr.getch()
            if 32 <= replace_key <= 126:
                self.buffer.save_state("Replace selection")
                replacement = chr(replace_key)
                for row, col in self.selected_cells:
                    self.buffer.set_cell(row, col, replacement, save_state=False)
                self.exit_visual_mode()
                self.status_message = f"Replaced {len(self.selected_cells)} cells with '{replacement}'"
            else:
                self.status_message = "Replace cancelled"
        elif key == curses.KEY_F1:
            self.show_help()
        # Navigation updates selection
        elif key in [ord('h'), ord('j'), ord('k'), ord('l'), 
                     curses.KEY_LEFT, curses.KEY_DOWN, curses.KEY_UP, curses.KEY_RIGHT]:
            if key == ord('h') or key == curses.KEY_LEFT:
                self.move_cursor(0, -1)
            elif key == ord('j') or key == curses.KEY_DOWN:
                self.move_cursor(1, 0)
            elif key == ord('k') or key == curses.KEY_UP:
                self.move_cursor(-1, 0)
            elif key == ord('l') or key == curses.KEY_RIGHT:
                self.move_cursor(0, 1)
            self.update_visual_selection()
        
        return True
    
    def select_all_data(self):
        """Select all data cells"""
        if not self.buffer.data or not self.buffer.headers:
            self.selected_cells.clear()
            self.status_message = "No data to select"
            return
        
        self.selected_cells.clear()
        
        # Select all data cells
        for row in range(len(self.buffer.data)):
            for col in range(len(self.buffer.headers)):
                self.selected_cells.add((row, col))
        
        # Update visual start and cursor to encompass all data
        self.visual_start = (0, 0)
        self.cursor_row = len(self.buffer.data) - 1
        self.cursor_col = len(self.buffer.headers) - 1
        
        self.adjust_scroll()
        self.status_message = f"Selected all data ({len(self.buffer.data)} rows × {len(self.buffer.headers)} cols)"
    
    def handle_search_mode(self, key: int) -> bool:
        """Handle search mode key presses"""
        if key == 27:  # ESC
            self.mode = 'NORMAL'
            self.command_buffer = ""
            self.status_message = "Search cancelled"
        elif key == 10 or key == 13:  # Enter
            if self.command_buffer:
                self.perform_search(self.command_buffer, forward=True)
            self.mode = 'NORMAL'
            self.command_buffer = ""
        elif key == curses.KEY_BACKSPACE or key == 127:
            self.command_buffer = self.command_buffer[:-1]
            self.status_message = "/" + self.command_buffer
        elif key == curses.KEY_F1:
            self.show_help()
        elif 32 <= key <= 126:  # Printable characters
            self.command_buffer += chr(key)
            self.status_message = "/" + self.command_buffer
        
        return True
    
    def handle_fullscreen_edit_mode(self, key: int) -> bool:
        """Handle fullscreen edit mode with scrollable text editing and fast navigation"""
        if key == 27:  # ESC - cancel without saving
            self.exit_fullscreen_edit_mode(save=False)
            self.status_message = "Edit cancelled"
        elif key == 24:  # Ctrl+X - save and exit
            self.exit_fullscreen_edit_mode(save=True)
            self.status_message = "Text saved"
        elif key == 19:  # Ctrl+S - save without exiting
            text_content = '\n'.join(self.edit_lines)
            self.buffer.set_cell(self.cursor_row, self.cursor_col, text_content)
            self.status_message = "Text saved (Ctrl+X to exit)"
        elif key == curses.KEY_F1:
            self.show_help()
        # Fast navigation shortcuts
        elif key == 23:  # Ctrl+W - next word
            self.move_to_next_word()
        elif key == 2:   # Ctrl+B - previous word
            self.move_to_prev_word()
        elif key == 6:   # Ctrl+F - page right (horizontal scroll)
            self.edit_scroll_col += 20
            self.adjust_edit_scroll()
        elif key == 1:   # Ctrl+A - beginning of line
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        elif key == 5:   # Ctrl+E - end of line
            self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
            self.adjust_edit_scroll()
        elif key == 3:   # Ctrl+C - center screen
            self.center_edit_screen()
        # Ctrl+H/J/K/L navigation (vim-style with Ctrl to avoid typing conflicts)
        elif key == 8:   # Ctrl+H - left
            if self.edit_cursor_col > 0:
                self.edit_cursor_col -= 1
            elif self.edit_cursor_line > 0:
                self.edit_cursor_line -= 1
                self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
            self.adjust_edit_scroll()
        # Editing operations - Multiple ways to split lines
        elif key == 13:  # Return/Enter key - create new line (works anywhere on line)
            current_line = self.edit_lines[self.edit_cursor_line]
            left_part = current_line[:self.edit_cursor_col]
            right_part = current_line[self.edit_cursor_col:]
            
            self.edit_lines[self.edit_cursor_line] = left_part
            self.edit_lines.insert(self.edit_cursor_line + 1, right_part)
            
            self.edit_cursor_line += 1
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
            self.status_message = f"Split line with Enter - now on line {self.edit_cursor_line + 1}"
        elif key == 18:  # Ctrl+R - reliable line split alternative
            current_line = self.edit_lines[self.edit_cursor_line]
            left_part = current_line[:self.edit_cursor_col]
            right_part = current_line[self.edit_cursor_col:]
            
            self.edit_lines[self.edit_cursor_line] = left_part
            self.edit_lines.insert(self.edit_cursor_line + 1, right_part)
            
            self.edit_cursor_line += 1
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
            self.status_message = f"Split line with Ctrl+R - now on line {self.edit_cursor_line + 1}"
        elif key == 21:  # Ctrl+U - insert new line ABOVE current line (safe key)
            self.edit_lines.insert(self.edit_cursor_line, "")
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
            self.status_message = f"Inserted line above - now on line {self.edit_cursor_line + 1}"
        elif key == 9:   # Ctrl+I (Tab) - insert new line BELOW current line  
            self.edit_lines.insert(self.edit_cursor_line + 1, "")
            self.edit_cursor_line += 1
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        elif key == 10 and self.edit_cursor_col == 0:  # Line feed at start of line - also new line
            current_line = self.edit_lines[self.edit_cursor_line]
            left_part = current_line[:self.edit_cursor_col]
            right_part = current_line[self.edit_cursor_col:]
            
            self.edit_lines[self.edit_cursor_line] = left_part
            self.edit_lines.insert(self.edit_cursor_line + 1, right_part)
            
            self.edit_cursor_line += 1
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        elif key == 10:  # Ctrl+J - down (only when NOT at start of line)
            # On Mac, key 10 might be Enter, so we need to be more careful
            # Only treat as Ctrl+J if we're not at column 0 (where Enter would make sense)
            if self.edit_cursor_col > 5 and self.edit_cursor_line < len(self.edit_lines) - 1:
                self.edit_cursor_line += 1
                if self.edit_cursor_col > len(self.edit_lines[self.edit_cursor_line]):
                    self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
                self.adjust_edit_scroll()
            # Otherwise, let it fall through to be handled as Enter below
        elif key == 11:  # Ctrl+K - up
            if self.edit_cursor_line > 0:
                self.edit_cursor_line -= 1
                if self.edit_cursor_col > len(self.edit_lines[self.edit_cursor_line]):
                    self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
                self.adjust_edit_scroll()
        elif key == 12:  # Ctrl+L - right
            if self.edit_cursor_col < len(self.edit_lines[self.edit_cursor_line]):
                self.edit_cursor_col += 1
            elif self.edit_cursor_line < len(self.edit_lines) - 1:
                self.edit_cursor_line += 1
                self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        # Arrow keys still work for navigation
        elif key == curses.KEY_UP:
            if self.edit_cursor_line > 0:
                self.edit_cursor_line -= 1
                # Adjust cursor column if line is shorter
                if self.edit_cursor_col > len(self.edit_lines[self.edit_cursor_line]):
                    self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
                self.adjust_edit_scroll()
        elif key == curses.KEY_DOWN:
            if self.edit_cursor_line < len(self.edit_lines) - 1:
                self.edit_cursor_line += 1
                # Adjust cursor column if line is shorter
                if self.edit_cursor_col > len(self.edit_lines[self.edit_cursor_line]):
                    self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
                self.adjust_edit_scroll()
        elif key == curses.KEY_LEFT:
            if self.edit_cursor_col > 0:
                self.edit_cursor_col -= 1
            elif self.edit_cursor_line > 0:
                # Move to end of previous line
                self.edit_cursor_line -= 1
                self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
            self.adjust_edit_scroll()
        elif key == curses.KEY_RIGHT:
            if self.edit_cursor_col < len(self.edit_lines[self.edit_cursor_line]):
                self.edit_cursor_col += 1
            elif self.edit_cursor_line < len(self.edit_lines) - 1:
                # Move to beginning of next line
                self.edit_cursor_line += 1
                self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        elif key == curses.KEY_HOME or key == ord('0'):
            self.edit_cursor_col = 0
            self.adjust_edit_scroll()
        elif key == curses.KEY_END or key == ord('$'):
            self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
            self.adjust_edit_scroll()
        elif key == curses.KEY_PPAGE:  # Page Up
            self.edit_cursor_line = max(0, self.edit_cursor_line - 10)
            self.adjust_edit_scroll()
        elif key == curses.KEY_NPAGE:  # Page Down
            self.edit_cursor_line = min(len(self.edit_lines) - 1, self.edit_cursor_line + 10)
            self.adjust_edit_scroll()
        # Ctrl-based horizontal scrolling (no conflicts with typing)
        elif key == 4:   # Ctrl+D - scroll left
            self.edit_scroll_col = max(0, self.edit_scroll_col - 10)
            self.adjust_edit_scroll()
        elif key == 6:   # Ctrl+F - scroll right
            self.edit_scroll_col += 10
            self.adjust_edit_scroll()
        elif key == 16:  # Ctrl+P - page scroll left
            self.edit_scroll_col = max(0, self.edit_scroll_col - 20)
            self.adjust_edit_scroll()
        elif key == 14:  # Ctrl+N - page scroll right
            self.edit_scroll_col += 20
            self.adjust_edit_scroll()
        # Editing
        elif key == curses.KEY_BACKSPACE or key == 127:
            if self.edit_cursor_col > 0:
                # Delete character before cursor
                line = self.edit_lines[self.edit_cursor_line]
                self.edit_lines[self.edit_cursor_line] = line[:self.edit_cursor_col-1] + line[self.edit_cursor_col:]
                self.edit_cursor_col -= 1
            elif self.edit_cursor_line > 0:
                # Join with previous line
                prev_line = self.edit_lines[self.edit_cursor_line - 1]
                current_line = self.edit_lines[self.edit_cursor_line]
                self.edit_cursor_col = len(prev_line)
                self.edit_lines[self.edit_cursor_line - 1] = prev_line + current_line
                del self.edit_lines[self.edit_cursor_line]
                self.edit_cursor_line -= 1
            self.adjust_edit_scroll()
        elif key == curses.KEY_DC:  # Delete key
            line = self.edit_lines[self.edit_cursor_line]
            if self.edit_cursor_col < len(line):
                # Delete character at cursor
                self.edit_lines[self.edit_cursor_line] = line[:self.edit_cursor_col] + line[self.edit_cursor_col+1:]
            elif self.edit_cursor_line < len(self.edit_lines) - 1:
                # Join with next line
                next_line = self.edit_lines[self.edit_cursor_line + 1]
                self.edit_lines[self.edit_cursor_line] += next_line
                del self.edit_lines[self.edit_cursor_line + 1]
        elif 32 <= key <= 126:  # Printable characters
            # Insert character at cursor position
            line = self.edit_lines[self.edit_cursor_line]
            self.edit_lines[self.edit_cursor_line] = line[:self.edit_cursor_col] + chr(key) + line[self.edit_cursor_col:]
            self.edit_cursor_col += 1
            self.adjust_edit_scroll()
        
        return True
    
    def adjust_edit_scroll(self):
        """Adjust scroll position for fullscreen edit mode with horizontal scrolling"""
        height, width = self.stdscr.getmaxyx()
        visible_lines = height - 4  # Leave space for header and status
        visible_cols = width - 6   # Leave space for line numbers
        
        # Vertical scrolling
        if self.edit_cursor_line < self.edit_scroll_line:
            self.edit_scroll_line = self.edit_cursor_line
        elif self.edit_cursor_line >= self.edit_scroll_line + visible_lines:
            self.edit_scroll_line = self.edit_cursor_line - visible_lines + 1
        
        # Horizontal scrolling
        if self.edit_cursor_col < self.edit_scroll_col:
            self.edit_scroll_col = self.edit_cursor_col
        elif self.edit_cursor_col >= self.edit_scroll_col + visible_cols:
            self.edit_scroll_col = self.edit_cursor_col - visible_cols + 1
        
        # Ensure scroll is within bounds
        self.edit_scroll_line = max(0, min(self.edit_scroll_line, max(0, len(self.edit_lines) - visible_lines)))
        self.edit_scroll_col = max(0, self.edit_scroll_col)
    
    def move_to_next_word(self):
        """Move cursor to next word boundary"""
        current_line = self.edit_lines[self.edit_cursor_line]
        pos = self.edit_cursor_col
        
        # Skip current word
        while pos < len(current_line) and current_line[pos].isalnum():
            pos += 1
        # Skip whitespace
        while pos < len(current_line) and current_line[pos].isspace():
            pos += 1
        
        # If we're at end of line, move to next line
        if pos >= len(current_line) and self.edit_cursor_line < len(self.edit_lines) - 1:
            self.edit_cursor_line += 1
            self.edit_cursor_col = 0
        else:
            self.edit_cursor_col = pos
        
        self.adjust_edit_scroll()
    
    def move_to_prev_word(self):
        """Move cursor to previous word boundary"""
        current_line = self.edit_lines[self.edit_cursor_line]
        pos = self.edit_cursor_col
        
        # Move back one position
        if pos > 0:
            pos -= 1
        elif self.edit_cursor_line > 0:
            # Move to end of previous line
            self.edit_cursor_line -= 1
            self.edit_cursor_col = len(self.edit_lines[self.edit_cursor_line])
            self.adjust_edit_scroll()
            return
        
        current_line = self.edit_lines[self.edit_cursor_line]
        
        # Skip whitespace
        while pos > 0 and current_line[pos].isspace():
            pos -= 1
        # Skip word
        while pos > 0 and current_line[pos-1].isalnum():
            pos -= 1
        
        self.edit_cursor_col = pos
        self.adjust_edit_scroll()
    
    def move_to_word_end(self):
        """Move cursor to end of current word"""
        current_line = self.edit_lines[self.edit_cursor_line]
        pos = self.edit_cursor_col
        
        # If not on a word character, find next word
        if pos < len(current_line) and not current_line[pos].isalnum():
            while pos < len(current_line) and not current_line[pos].isalnum():
                pos += 1
        
        # Move to end of word
        while pos < len(current_line) and current_line[pos].isalnum():
            pos += 1
        
        # Move back one to be on the last character of the word
        if pos > 0:
            pos -= 1
        
        self.edit_cursor_col = pos
        self.adjust_edit_scroll()
    
    def goto_line_in_edit(self):
        """Go to specific line number in edit mode"""
        height, width = self.stdscr.getmaxyx()
        
        # Show input prompt
        prompt = f"Go to line (1-{len(self.edit_lines)}): "
        input_buffer = ""
        
        while True:
            # Clear bottom area and show prompt
            try:
                self.stdscr.addstr(height-1, 0, " " * (width-1))
                self.stdscr.addstr(height-1, 0, prompt + input_buffer, curses.A_REVERSE)
                self.stdscr.refresh()
            except curses.error:
                pass
            
            key = self.stdscr.getch()
            
            if key == 27:  # ESC - cancel
                break
            elif key == 10 or key == 13:  # Enter - execute
                try:
                    line_num = int(input_buffer) - 1  # Convert to 0-based
                    if 0 <= line_num < len(self.edit_lines):
                        self.edit_cursor_line = line_num
                        self.edit_cursor_col = 0
                        self.adjust_edit_scroll()
                        self.status_message = f"Moved to line {line_num + 1}"
                    else:
                        self.status_message = f"Line {input_buffer} out of range"
                except ValueError:
                    self.status_message = "Invalid line number"
                break
            elif key == curses.KEY_BACKSPACE or key == 127:
                input_buffer = input_buffer[:-1]
            elif key.isdigit():
                input_buffer += chr(key)
    
    def center_edit_screen(self):
        """Center the current line on screen"""
        height, width = self.stdscr.getmaxyx()
        visible_lines = height - 4
        
        # Center current line
        target_scroll = max(0, self.edit_cursor_line - visible_lines // 2)
        self.edit_scroll_line = min(target_scroll, max(0, len(self.edit_lines) - visible_lines))
        self.adjust_edit_scroll()
    
    def show_sheet_selector(self, sheet_names: List[str], file_path: str) -> Optional[str]:
        """Show interactive sheet selection dialog"""
        height, width = self.stdscr.getmaxyx()
        selected_index = 0
        
        # Show sheet selection interface
        while True:
            self.stdscr.clear()
            
            # Title
            title = f"Select Excel Sheet from {Path(file_path).name}"
            try:
                self.stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD | curses.A_REVERSE)
            except curses.error:
                pass
            
            # Instructions
            instructions = "Use ↑↓ or jk to navigate, Enter to select, Esc to cancel"
            try:
                self.stdscr.addstr(2, (width - len(instructions)) // 2, instructions, curses.A_DIM)
            except curses.error:
                pass
            
            # Sheet list
            start_row = 4
            for i, sheet_name in enumerate(sheet_names):
                row = start_row + i
                if row >= height - 2:  # Don't go too close to bottom
                    break
                
                # Truncate long sheet names
                display_name = sheet_name[:width - 10] if len(sheet_name) > width - 10 else sheet_name
                
                try:
                    if i == selected_index:
                        # Highlight selected sheet
                        self.stdscr.addstr(row, 4, f"> {display_name}", curses.A_REVERSE | curses.A_BOLD)
                    else:
                        self.stdscr.addstr(row, 4, f"  {display_name}")
                except curses.error:
                    pass
            
            # Show sheet info if available
            if selected_index < len(sheet_names):
                info_text = f"Selected: {sheet_names[selected_index]}"
                try:
                    self.stdscr.addstr(height - 3, 4, info_text, curses.A_DIM)
                except curses.error:
                    pass
            
            # Status
            status = f"Sheet {selected_index + 1} of {len(sheet_names)} | Numbers 1-{min(9, len(sheet_names))} for quick select"
            try:
                self.stdscr.addstr(height - 1, 0, status[:width-1], curses.A_DIM)
            except curses.error:
                pass
            
            self.stdscr.refresh()
            
            # Handle input
            key = self.stdscr.getch()
            
            if key == 27:  # ESC - cancel
                return None
            elif key == 10 or key == 13:  # Enter - select
                return sheet_names[selected_index]
            elif key == curses.KEY_UP or key == ord('k'):
                selected_index = max(0, selected_index - 1)
            elif key == curses.KEY_DOWN or key == ord('j'):
                selected_index = min(len(sheet_names) - 1, selected_index + 1)
            elif key == ord('q'):  # Quick quit
                return None
            elif ord('1') <= key <= ord('9'):
                # Quick number selection
                num = key - ord('0')
                if 1 <= num <= len(sheet_names):
                    return sheet_names[num - 1]
    
    def draw_fullscreen_edit(self, stdscr):
        """Draw fullscreen scrollable text editor with horizontal scrolling"""
        height, width = stdscr.getmaxyx()
        visible_lines = height - 4
        visible_cols = width - 6  # Account for line numbers
        
        stdscr.clear()
        
        # Header with scroll position info
        cell_ref = f"{chr(65 + self.cursor_col % 26)}{self.cursor_row + 1}"
        col_name = ""
        if self.cursor_col < len(self.buffer.headers):
            col_name = f" ({self.buffer.headers[self.cursor_col]})"
        
        header = f"FULLSCREEN EDIT - Cell {cell_ref}{col_name} | Line {self.edit_cursor_line + 1}/{len(self.edit_lines)} | Col {self.edit_cursor_col + 1}"
        if self.edit_scroll_col > 0:
            header += f" | H-Scroll: {self.edit_scroll_col}"
        
        try:
            stdscr.addstr(0, 0, header[:width-1], curses.A_BOLD | curses.A_REVERSE)
        except curses.error:
            pass
        
        # Text content with line numbers and horizontal scrolling
        for i in range(visible_lines):
            line_idx = self.edit_scroll_line + i
            screen_row = 1 + i
            
            if line_idx < len(self.edit_lines):
                line_num = f"{line_idx + 1:4d} "
                content = self.edit_lines[line_idx]
                
                # Apply horizontal scrolling
                start_col = self.edit_scroll_col
                end_col = start_col + visible_cols
                display_content = content[start_col:end_col]
                
                # Pad to full width for proper highlighting
                display_content = display_content.ljust(visible_cols)[:visible_cols]
                
                try:
                    # Line number
                    if line_idx == self.edit_cursor_line:
                        stdscr.addstr(screen_row, 0, line_num, curses.A_BOLD)
                    else:
                        stdscr.addstr(screen_row, 0, line_num, curses.A_DIM)
                    
                    # Content with horizontal scrolling
                    if line_idx == self.edit_cursor_line:
                        # Highlight current line
                        stdscr.addstr(screen_row, 5, display_content, curses.A_STANDOUT)
                        
                        # Show cursor position if visible
                        cursor_screen_col = self.edit_cursor_col - self.edit_scroll_col
                        if 0 <= cursor_screen_col < len(display_content.rstrip()):
                            try:
                                char_at_cursor = display_content[cursor_screen_col]
                                stdscr.addstr(screen_row, 5 + cursor_screen_col, char_at_cursor, 
                                            curses.A_REVERSE | curses.A_BOLD)
                            except curses.error:
                                pass
                        elif cursor_screen_col == len(content[start_col:end_col]) and cursor_screen_col < visible_cols:
                            # Cursor at end of visible line content
                            try:
                                stdscr.addstr(screen_row, 5 + cursor_screen_col, " ", 
                                            curses.A_REVERSE | curses.A_BOLD)
                            except curses.error:
                                pass
                    else:
                        stdscr.addstr(screen_row, 5, display_content.rstrip())
                        
                except curses.error:
                    pass
        
        # Enhanced status and help with horizontal scroll info
        help_y = height - 2
        help_text = "arrows/Ctrl+hjkl:move Ctrl+W/B:word Enter/Ctrl+R:split-line Ctrl+U:line-above Ctrl+I:line-below"
        try:
            stdscr.addstr(help_y, 0, help_text[:width-1], curses.A_DIM)
        except curses.error:
            pass
        
        # Status line with enhanced info
        status_y = height - 1
        status_text = self.status_message
        # Add cursor and scroll position info
        status_text += f" | Pos: {self.edit_cursor_line + 1}:{self.edit_cursor_col + 1}"
        if self.edit_scroll_col > 0:
            status_text += f" | H-Offset: {self.edit_scroll_col}"
        
        # Show if content extends beyond visible area
        current_line = self.edit_lines[self.edit_cursor_line] if self.edit_cursor_line < len(self.edit_lines) else ""
        if len(current_line) > self.edit_scroll_col + visible_cols:
            status_text += " →"
        if self.edit_scroll_col > 0:
            status_text += " ←"
        
        try:
            stdscr.addstr(status_y, 0, status_text[:width-1])
        except curses.error:
            pass
        
        stdscr.refresh()
    
    def handle_command_mode(self, key: int) -> bool:
        """Enhanced command mode key presses"""
        if key == 27:  # ESC
            self.mode = 'NORMAL'
            self.command_buffer = ""
            self.status_message = "Ready"
        elif key == 10 or key == 13:  # Enter
            result = self.execute_command(self.command_buffer)
            self.mode = 'NORMAL'
            self.command_buffer = ""
            return result  # May return False to quit
        elif key == curses.KEY_BACKSPACE or key == 127:
            self.command_buffer = self.command_buffer[:-1]
            self.status_message = ":" + self.command_buffer
        elif key == 9:  # Tab completion
            self.tab_complete_command()
        elif key == curses.KEY_F1:
            self.show_help()
        elif 32 <= key <= 126:  # Printable characters
            self.command_buffer += chr(key)
            self.status_message = ":" + self.command_buffer
        
        return True
    
    def tab_complete_command(self):
        """Basic tab completion for commands"""
        commands = ['write', 'quit', 'help', 'new', 'edit', 'goto', 'sort', 'find', 'replace', 'compress', 'ccsv']
        
        if self.command_buffer:
            matches = [cmd for cmd in commands if cmd.startswith(self.command_buffer)]
            if len(matches) == 1:
                self.command_buffer = matches[0]
                self.status_message = ":" + self.command_buffer
            elif len(matches) > 1:
                self.status_message = f":{self.command_buffer} ({', '.join(matches)})"
    
    def handle_insert_mode(self, key: int) -> bool:
        """Enhanced insert mode with better editing"""
        if key == 27:  # ESC
            self.exit_insert_mode()
        elif key == 10 or key == 13:  # Enter
            self.exit_insert_mode()
            self.move_cursor(1, 0)  # Move to next row
        elif key == curses.KEY_BACKSPACE or key == 127:
            self.edit_buffer = self.edit_buffer[:-1]
        elif key == 9:  # Tab - move to next cell
            self.exit_insert_mode()
            self.move_cursor(0, 1)
            self.enter_insert_mode()
        elif key == 353:  # Shift+Tab - move to previous cell
            self.exit_insert_mode()
            self.move_cursor(0, -1)
            self.enter_insert_mode()
        elif key == curses.KEY_F1:
            self.show_help()
        # Navigation in insert mode (Excel-like)
        elif key == curses.KEY_LEFT and len(self.edit_buffer) == 0:
            self.exit_insert_mode()
            self.move_cursor(0, -1)
            self.enter_insert_mode()
        elif key == curses.KEY_RIGHT and len(self.edit_buffer) == 0:
            self.exit_insert_mode()
            self.move_cursor(0, 1)
            self.enter_insert_mode()
        elif key == curses.KEY_UP:
            self.exit_insert_mode()
            self.move_cursor(-1, 0)
            self.enter_insert_mode()
        elif key == curses.KEY_DOWN:
            self.exit_insert_mode()
            self.move_cursor(1, 0)
            self.enter_insert_mode()
        elif key == 1:  # Ctrl+A - select all in edit buffer
            # For now, just move to beginning
            pass
        elif key == 5:  # Ctrl+E - move to end
            # For now, just move to end
            pass
        elif 32 <= key <= 126:  # Printable characters
            self.edit_buffer += chr(key)
        
        return True
    
    def show_help(self):
        """Enhanced help screen with new features"""
        help_text = """
ENHANCED VIM CSV EDITOR - HELP
==============================

MODES:
------
NORMAL MODE (default):
  Navigation:   hjkl or arrow keys
  Fast nav:     w(right) b(left) 0(row start) $(row end) ^(first non-empty)
  Jump:         g(first row) G(last row) [count]G(go to line)
               H(top of screen) M(middle) L(bottom)
  Page:         Ctrl+f(page down) Ctrl+b(page up)
               Ctrl+d(half down) Ctrl+u(half up)
  
  Edit:         i(insert) a(append) I(fullscreen edit) A(goto cell) R(replace mode)
  Rows:         o(insert below) O(insert above) D(duplicate row)
  Delete:       x(clear cell) d(delete row) X(delete column)
  Undo/Redo:    u(undo) Ctrl+r(redo)
  Copy/Paste:   y(yank) p(paste) P(paste before)
  Visual:       v(visual) V(visual line)
  Columns:      c(insert right) C(insert left) e(edit name)
  Search:       /(search forward) ?(search backward) n(next) N(prev)
  Sort:         s(sort asc) S(sort desc)
  Width:        =(auto-resize) +(wider) -(narrower)
  Command:      :(command mode)
  Help:         F1 or H

ENHANCED FEATURES:
------------------
• Smart sorting:     Detects numeric vs text data
• Undo/Redo:         Full operation history with Ctrl+r
• Search/Replace:    Regex support, highlight matches
• Auto-resize:       Smart column width adjustment
• Visual line:       Select entire rows with V
• Duplicate rows:    D command for quick row copying
• Better navigation: H/M/L for screen positioning
• Tab completion:    In command mode for faster typing

INSERT MODE:
  Edit cell content with live preview
  Save & move:  Enter(down) Tab(right) Shift+Tab(left)
  Navigate:     Arrow keys move between cells (when buffer empty)
  Advanced:     Ctrl+a(select all) Ctrl+e(end of line)
  Cancel:       Esc
  Help:         F1

FULLSCREEN EDIT MODE (I key):
  Full-screen scrollable text editor for large content
  Navigation:   Arrow keys OR Ctrl+H/J/K/L, Page Up/Down, Home/End
  Fast Nav:     Ctrl+W/B (word movement), Ctrl+F/D (horizontal scroll)
  Line Control: Enter/Ctrl+R (split line), Ctrl+U (insert above), Ctrl+I (insert below)
  Jump:         Ctrl+C (center screen)
  Line ops:     Ctrl+A(line start) Ctrl+E(line end)
  H-Scroll:     Ctrl+F/D (scroll 10 chars), Ctrl+P/N (scroll 20 chars)
  Edit:         Type ANY characters freely, powerful line insertion
  Delete:       Backspace (before cursor), Delete (at cursor)
  Save:         Ctrl+S (save and continue) Ctrl+X (save and exit)
  Cancel:       Esc (discard changes)
  Features:     Line numbers, cursor position, unlimited scrolling, NO typing conflicts

VISUAL MODE:
  Extend:       hjkl or arrows to select range
  Copy:         y(yank selection)
  Delete:       d(delete selection)
  Change:       c(delete and insert)
  Replace:      r(replace all with char)
  Select All:   a(select all data)
  Line mode:    V(select entire rows)
  Cancel:       Esc

SEARCH MODE:
  Search:       /(forward) ?(backward)
  Navigate:     n(next match) N(previous match)
  Features:     Case-insensitive by default
  Cancel:       Esc

COMMAND MODE:
  File ops:     :w(save) :w filename(save as) :q(quit) :wq(save&quit)
  Edit:         :e filename(open) :new(new buffer)
  Navigation:   :goto A1 (go to cell)
  Sort:         :sort column_name [asc|desc]
  Search:       :find pattern :replace old new
  Excel:        :sheets(list sheets) :sheet(switch sheet)
  Compress:     :compress or :ccsv(create and save file as compressed csv for storage savings)
  Settings:     :set option value
  Help:         :help
  Completion:   Tab (complete commands)
  Cancel:       Esc

ADVANCED FEATURES:
------------------
• Smart Data Types:   Preserves int/float/string on save
• Visual Block Ops:   Edit multiple cells at once
• Flexible Widths:    Per-column width adjustment
• Search History:     Remembers last search pattern
• Auto-save:         Saves every 30 seconds automatically
• Type Detection:     Smart sorting based on data type
• Excel Compatibility: Saves as CSV but preserves data

KEYBOARD SHORTCUTS:
-------------------
F1           Help screen
Esc          Cancel/Normal mode
Tab          Next cell (insert mode)
Shift+Tab    Previous cell (insert mode)
Enter        Save cell & move down
Ctrl+S       Quick save (:w)
Ctrl+R       Redo
u            Undo
/            Search forward
?            Search backward
n            Next search result
N            Previous search result
=            Auto-resize all columns
+            Make column wider
-            Make column narrower

GOTO CELL (A command):
----------------------
A            Open goto dialog
Enter:       A1, B5, C10, etc.
Examples:    A1 (first cell), Z100 (column Z row 100)
            AA1 (column 27), AB5 (column 28 row 5)
            Can also use :goto A1 in command mode

COLUMN OPERATIONS:
------------------
c            Insert new column to the RIGHT of cursor
C            Insert new column to the LEFT of cursor  
X            Delete current column
e            Edit current column name
=            Auto-resize all columns
+            Increase current column width
-            Decrease current column width

ROW OPERATIONS:
---------------
o            Insert row below cursor
O            Insert row above cursor
d            Delete current row
D            Duplicate current row

CELL OPERATIONS:
----------------
i            Edit current cell
a            Append to current cell (same as i)
R            Replace mode (overwrite cell)
x            Clear current cell content
r            Replace single character
y            Copy current cell or selection
p            Paste at cursor
P            Paste before cursor

VISUAL OPERATIONS:
------------------
v            Start visual selection
V            Start visual line selection (entire rows)
y            Yank (copy) selection
d            Delete selection
c            Change selection (delete and enter insert)
r            Replace all selected cells with single character
a            Select all data

SEARCH & REPLACE:
-----------------
/pattern     Search forward for pattern
?pattern     Search backward for pattern
n            Go to next match
N            Go to previous match
:find pat    Search for pattern (command mode)
:replace old new  Replace old with new (future feature)

SORTING:
--------
s            Sort current column ascending
S            Sort current column descending
:sort col    Sort by column name
:sort col desc  Sort by column descending

Press any key to continue, 'q' to quit help, or use j/k to scroll...
"""
        
        # Show help in a modal-like interface
        self.show_scrollable_text("ENHANCED HELP", help_text)
    
    def edit_column_name(self):
        """Edit the name of the current column"""
        if not self.buffer.headers or self.cursor_col >= len(self.buffer.headers):
            self.status_message = "No column to edit"
            return
        
        height, width = self.stdscr.getmaxyx()
        current_name = self.buffer.headers[self.cursor_col]
        
        # Show input prompt
        prompt = f"Edit column name '{current_name}': "
        input_buffer = current_name  # Start with current name
        
        while True:
            # Clear bottom area and show prompt
            try:
                self.stdscr.addstr(height-1, 0, " " * (width-1))
                display_text = prompt + input_buffer
                if len(display_text) > width - 1:
                    # Truncate if too long
                    display_text = prompt + "..." + input_buffer[-(width-len(prompt)-6):]
                self.stdscr.addstr(height-1, 0, display_text, curses.A_REVERSE)
                self.stdscr.refresh()
            except curses.error:
                pass
            
            key = self.stdscr.getch()
            
            if key == 27:  # ESC - cancel
                self.status_message = "Column name edit cancelled"
                break
            elif key == 10 or key == 13:  # Enter - save
                new_name = input_buffer.strip()
                if new_name and new_name != current_name:
                    # Check for duplicate names
                    if new_name in self.buffer.headers:
                        self.status_message = f"Column name '{new_name}' already exists"
                    else:
                        self.buffer.save_state(f"Rename column {current_name} to {new_name}")
                        self.buffer.headers[self.cursor_col] = new_name
                        self.buffer.dirty = True
                        self.status_message = f"Column renamed to '{new_name}'"
                elif new_name == current_name:
                    self.status_message = "Column name unchanged"
                else:
                    self.status_message = "Column name cannot be empty"
                break
            elif key == curses.KEY_BACKSPACE or key == 127:
                input_buffer = input_buffer[:-1]
            elif 32 <= key <= 126:  # Printable characters
                input_buffer += chr(key)
                # Limit input length
                if len(input_buffer) > 50:
                    input_buffer = input_buffer[:50]
    
    def goto_cell(self):
        """Interactive goto cell functionality (Excel-like)"""
        height, width = self.stdscr.getmaxyx()
        
        # Show input prompt
        prompt = "Go to cell (e.g., A1, B5, C10): "
        input_buffer = ""
        
        while True:
            # Clear bottom area and show prompt
            try:
                self.stdscr.addstr(height-1, 0, " " * (width-1))
                self.stdscr.addstr(height-1, 0, prompt + input_buffer, curses.A_REVERSE)
                self.stdscr.refresh()
            except curses.error:
                pass
            
            key = self.stdscr.getch()
            
            if key == 27:  # ESC - cancel
                self.status_message = "Goto cancelled"
                break
            elif key == 10 or key == 13:  # Enter - execute
                if input_buffer.strip():
                    if self.parse_and_goto_cell(input_buffer.strip()):
                        self.status_message = f"Moved to {input_buffer.strip()}"
                    else:
                        self.status_message = f"Invalid cell reference: {input_buffer.strip()}"
                else:
                    self.status_message = "Goto cancelled"
                break
            elif key == curses.KEY_BACKSPACE or key == 127:
                input_buffer = input_buffer[:-1]
            elif 32 <= key <= 126:  # Printable characters
                input_buffer += chr(key)
                # Limit input length
                if len(input_buffer) > 10:
                    input_buffer = input_buffer[:10]
    
    def parse_and_goto_cell(self, cell_ref: str) -> bool:
        """Parse cell reference (A1, B5, etc.) and move cursor"""
        try:
            cell_ref = cell_ref.upper().strip()
            
            # Parse column letter(s) and row number
            col_part = ""
            row_part = ""
            found_digit = False
            
            for char in cell_ref:
                if char.isalpha():
                    if found_digit:
                        return False  # Letters after digits not allowed
                    col_part += char
                elif char.isdigit():
                    found_digit = True
                    row_part += char
                else:
                    return False  # Invalid character
            
            if not col_part or not row_part:
                return False
            
            # Convert column letters to index (A=0, B=1, ..., Z=25, AA=26, etc.)
            col_index = 0
            for i, char in enumerate(reversed(col_part)):
                col_index += (ord(char) - ord('A') + 1) * (26 ** i)
            col_index -= 1  # Convert to 0-based
            
            # Convert row to index (1-based to 0-based)
            row_index = int(row_part) - 1
            
            # Validate bounds
            if (row_index < 0 or 
                col_index < 0 or 
                (self.buffer.data and row_index >= len(self.buffer.data)) or
                (self.buffer.headers and col_index >= len(self.buffer.headers))):
                return False
            
            # Move cursor
            self.cursor_row = row_index
            self.cursor_col = col_index
            self.adjust_scroll()
            return True
            
        except (ValueError, IndexError):
            return False
    
    def show_scrollable_text(self, title: str, text: str):
        """Show scrollable text in a modal interface"""
        lines = text.strip().split('\n')
        scroll_pos = 0
        
        while True:
            height, width = self.stdscr.getmaxyx() if hasattr(self, 'stdscr') else (24, 80)
            display_height = height - 4
            
            # Clear screen and draw border
            self.stdscr.clear()
            
            # Title
            title_text = f"=== {title} ==="
            try:
                self.stdscr.addstr(0, (width - len(title_text)) // 2, title_text, curses.A_BOLD)
            except curses.error:
                pass
            
            # Content
            for i in range(display_height):
                line_idx = scroll_pos + i
                if line_idx < len(lines):
                    line = lines[line_idx][:width-2]
                    try:
                        self.stdscr.addstr(1 + i, 1, line)
                    except curses.error:
                        pass
            
            # Scroll indicators and help
            if scroll_pos > 0:
                try:
                    self.stdscr.addstr(1, width-10, "↑ More ↑", curses.A_DIM)
                except curses.error:
                    pass
            
            if scroll_pos + display_height < len(lines):
                try:
                    self.stdscr.addstr(height-3, width-10, "↓ More ↓", curses.A_DIM)
                except curses.error:
                    pass
            
            # Bottom help line
            help_line = "j/k:scroll q/Esc:close Space/Enter:close"
            try:
                self.stdscr.addstr(height-1, 1, help_line, curses.A_REVERSE)
            except curses.error:
                pass
            
            self.stdscr.refresh()
            
            # Handle input
            key = self.stdscr.getch()
            if key == ord('q') or key == 27 or key == ord(' ') or key == 10 or key == 13:
                break
            elif key == ord('j') or key == curses.KEY_DOWN:
                if scroll_pos + display_height < len(lines):
                    scroll_pos += 1
            elif key == ord('k') or key == curses.KEY_UP:
                if scroll_pos > 0:
                    scroll_pos -= 1
            elif key == curses.KEY_NPAGE:  # Page down
                scroll_pos = min(scroll_pos + display_height, max(0, len(lines) - display_height))
            elif key == curses.KEY_PPAGE:  # Page up
                scroll_pos = max(0, scroll_pos - display_height)
        
        # Restore normal display
        self.status_message = "Help closed"
    
    def execute_command(self, command: str):
        """Execute enhanced vim-like commands"""
        cmd = command.strip()
        
        if cmd in ['q', 'quit']:
            return False
        elif cmd in ['w', 'write']:
            if self.buffer.stream_save():
                self.status_message = "File saved"
            else:
                self.status_message = "Save failed"
        elif cmd in ['wq', 'x']:
            if self.buffer.stream_save():
                return False
            else:
                self.status_message = "Save failed"
        elif cmd.startswith('w '):
            # Save to specific file
            filename = cmd[2:].strip()
            if self.buffer.stream_save(filename):
                self.status_message = f"Saved to {filename}"
            else:
                self.status_message = "Save failed"
        elif cmd.startswith('e '):
            # Edit/open file
            filename = cmd[2:].strip()
            if self.buffer.load_from_file(filename):
                self.cursor_row = self.cursor_col = 0
                self.scroll_row = self.scroll_col = 0
                self.status_message = f"Loaded {filename}"
            else:
                self.status_message = f"Failed to load {filename}"
        elif cmd == 'compress' or cmd == 'ccsv':
            start_time = time.time()
            if self.buffer.save_as_ccsv():
                compress_time = time.time() - start_time
                self.status_message = f"Compressed in {compress_time:.2f}s - ultra-fast loading enabled!"
            else:
                self.status_message = "Compression failed"

        elif cmd.startswith('saveas '):
            filename = cmd[7:].strip()
            start_time = time.time()
            if filename.endswith('.ccsv'):
                if self.buffer.save_as_ccsv(filename):
                    save_time = time.time() - start_time
                    self.status_message = f"Compressed to {filename} in {save_time:.2f}s"
                else:
                    self.status_message = "Compression save failed"
            else:
                if self.buffer.stream_save(filename):
                    save_time = time.time() - start_time
                    self.status_message = f"Saved to {filename} in {save_time:.2f}s"
                else:
                    self.status_message = "Save failed"

        elif cmd == 'perf' or cmd == 'performance':
            load_time = self.buffer.last_load_time
            save_time = self.buffer.last_save_time
            file_format = self.buffer.file_format.upper()
            self.status_message = f"Performance: Load={load_time:.2f}s Save={save_time:.2f}s Format={file_format}"
        elif cmd == 'new':
            # New file
            self.buffer = CSVBuffer()
            self.cursor_row = self.cursor_col = 0
            self.scroll_row = self.scroll_col = 0
            self.status_message = "New buffer"
        elif cmd in ['help', 'h']:
            # Show help
            self.show_help()
        elif cmd.startswith('goto ') or cmd.startswith('g '):
            # Goto cell command
            cell_ref = cmd.split(' ', 1)[1] if ' ' in cmd else ""
            if cell_ref and self.parse_and_goto_cell(cell_ref):
                self.status_message = f"Moved to {cell_ref}"
            else:
                self.status_message = f"Invalid cell reference: {cell_ref}"
        elif cmd.startswith('sort '):
            # Sort by column name
            parts = cmd.split()
            if len(parts) >= 2:
                col_name = parts[1]
                ascending = True
                if len(parts) >= 3 and parts[2].lower() in ['desc', 'descending']:
                    ascending = False
                
                # Find column index
                try:
                    col_idx = self.buffer.headers.index(col_name)
                    if self.buffer.sort_by_column(col_idx, ascending):
                        direction = "ascending" if ascending else "descending"
                        self.status_message = f"Sorted by {col_name} ({direction})"
                    else:
                        self.status_message = "Sort failed"
                except ValueError:
                    self.status_message = f"Column '{col_name}' not found"
            else:
                self.status_message = "Usage: :sort column_name [asc|desc]"
        elif cmd.startswith('find '):
            # Search command
            pattern = cmd[5:].strip()
            if pattern:
                results = self.search_manager.search(self.buffer.data, pattern)
                if results:
                    first_result = self.search_manager.next_result()
                    if first_result:
                        self.cursor_row, self.cursor_col = first_result
                        self.adjust_scroll()
                        self.status_message = f"Found {len(results)} matches"
                else:
                    self.status_message = "Pattern not found"
            else:
                self.status_message = "Usage: :find pattern"
        elif cmd.startswith('sheet '):
            # Switch to different sheet in Excel file
            if hasattr(self.buffer, 'current_sheet'):
                original_file = self.buffer.file_path
                if original_file and original_file.endswith('.csv'):
                    # Try to find the original Excel file
                    base_name = original_file.rsplit('_', 1)[0]
                    excel_file = base_name + '.xlsx'
                    if Path(excel_file).exists():
                        if self.buffer.load_from_file(excel_file):
                            self.cursor_row = self.cursor_col = 0
                            self.scroll_row = self.scroll_col = 0
                            self.status_message = f"Switched to sheet {self.buffer.current_sheet}"
                        else:
                            self.status_message = "Failed to load Excel file"
                    else:
                        self.status_message = "Original Excel file not found"
                else:
                    self.status_message = "Not working with an Excel file"
            else:
                self.status_message = "No sheet information available"
        elif cmd == 'sheets':
            # List available sheets
            if hasattr(self.buffer, 'current_sheet'):
                original_file = self.buffer.file_path
                if original_file and original_file.endswith('.csv'):
                    base_name = original_file.rsplit('_', 1)[0]
                    excel_file = base_name + '.xlsx'
                    if Path(excel_file).exists():
                        try:
                            excel_file_obj = pd.ExcelFile(excel_file, engine='openpyxl')
                            sheet_names = excel_file_obj.sheet_names
                            current = self.buffer.current_sheet
                            sheet_list = ', '.join([f"*{s}*" if s == current else s for s in sheet_names])
                            self.status_message = f"Sheets: {sheet_list} (* = current)"
                        except:
                            self.status_message = "Failed to read Excel file"
                    else:
                        self.status_message = "Original Excel file not found"
                else:
                    self.status_message = "Not working with an Excel file"
            else:
                self.status_message = "No sheet information available"
        elif cmd == 'resize' or cmd == 'autowidth':
            # Auto-resize columns
            self.buffer.auto_resize_columns()
            self.status_message = "Auto-resized columns"
        elif cmd.startswith('set '):
            # Settings command (placeholder for future)
            setting_parts = cmd[4:].strip().split()
            if len(setting_parts) >= 2:
                setting_name = setting_parts[0]
                setting_value = setting_parts[1]
                self.status_message = f"Setting '{setting_name}' to '{setting_value}' (not implemented)"
            else:
                self.status_message = "Usage: :set option value"
        else:
            self.status_message = f"Unknown command: {cmd}"
        
        return True
    
    def draw_screen(self, stdscr):
        """Enhanced screen drawing with better visual feedback and fullscreen edit mode"""
        height, width = stdscr.getmaxyx()
        
        # Complete screen clear to prevent any multiline spillover artifacts
        stdscr.clear()
        
        # Handle fullscreen edit mode
        if self.mode == 'FULLSCREEN_EDIT':
            self.draw_fullscreen_edit(stdscr)
            return
        
        self.visible_rows = height - 6  # Leave space for header, edit bar, and footer
        
        # Calculate visible columns based on dynamic widths
        total_width = 6  # Row number space
        visible_cols = 0
        for i in range(self.scroll_col, len(self.buffer.headers)):
            col_width = self.buffer.get_column_width(i)
            if total_width + col_width > width:
                break
            total_width += col_width
            visible_cols += 1
        
        self.visible_cols = max(1, visible_cols)
        
        stdscr.clear()
        
        # Header line with enhanced info
        file_display = self.buffer.file_path or '[No Name]'
        if hasattr(self.buffer, 'current_sheet') and self.buffer.current_sheet:
            file_display += f" (Sheet: {self.buffer.current_sheet})"
        
        header_text = f"File: {file_display} | "
        header_text += f"Rows: {len(self.buffer.data)} | "
        header_text += f"Cols: {len(self.buffer.headers)} | "
        header_text += f"Pos: {chr(65 + self.cursor_col % 26)}{self.cursor_row + 1} | "
        header_text += f"Mode: {self.mode}"
        header_text += self.buffer.get_compression_info()
        
        if self.buffer.dirty:
            header_text += " [+]"
        
        if self.buffer.undo_manager.can_undo():
            header_text += " [U]"
        
        if self.search_manager.search_results:
            header_text += f" [{len(self.search_manager.search_results)} matches]"
        
        try:
            stdscr.addstr(0, 0, header_text[:width-1])
        except curses.error:
            pass
        
        # Enhanced edit bar with more info
        edit_bar_label = f"Cell {chr(65 + self.cursor_col % 26)}{self.cursor_row + 1}"
        if self.cursor_col < len(self.buffer.headers):
            edit_bar_label += f" ({self.buffer.headers[self.cursor_col]})"
        edit_bar_label += ": "
        
        edit_bar_content = ""
        if self.mode == 'INSERT':
            edit_bar_content = self.edit_buffer
        elif self.buffer.data and self.cursor_row < len(self.buffer.data):
            raw_content = self.buffer.get_cell(self.cursor_row, self.cursor_col)
            # Only show first line in edit bar to prevent spillover
            lines = raw_content.split('\n')
            first_line = lines[0] if lines else ""
            # Add indicator if there are multiple lines
            if len(lines) > 1:
                edit_bar_content = first_line + " ↵"
            else:
                edit_bar_content = first_line
        
        # Sanitize edit bar content to absolutely prevent newlines
        edit_bar_content = edit_bar_content.replace('\n', '').replace('\r', '')
        
        edit_bar = edit_bar_label + edit_bar_content
        try:
            if self.mode == 'INSERT':
                stdscr.addstr(1, 0, edit_bar[:width-1], curses.A_REVERSE)
            else:
                stdscr.addstr(1, 0, edit_bar[:width-1])
        except curses.error:
            pass
        
        # Enhanced column headers with sorting indicators
        if self.buffer.headers:
            try:
                # Draw row number space first
                stdscr.addstr(2, 0, " " * 6)
                
                x_offset = 6
                for i in range(self.visible_cols):
                    col_idx = self.scroll_col + i
                    if col_idx >= len(self.buffer.headers):
                        break
                    
                    col_width = self.buffer.get_column_width(col_idx)
                    header = self.buffer.headers[col_idx]
                    
                    # Truncate header if needed
                    display_header = header[:col_width-2]
                    if len(header) > col_width-2:
                        display_header = display_header[:-1] + "…"
                    
                    # Add sorting indicator
                    if hasattr(self.buffer, 'sort_column') and self.buffer.sort_column == col_idx:
                        sort_indicator = " ↑" if self.buffer.sort_ascending else " ↓"
                        display_header = (display_header + sort_indicator)[:col_width-1]
                    
                    cell_text = f"{display_header:^{col_width}}"
                    
                    # Highlight current column header
                    if col_idx == self.cursor_col:
                        stdscr.addstr(2, x_offset, cell_text, curses.A_BOLD | curses.A_REVERSE)
                    else:
                        stdscr.addstr(2, x_offset, cell_text, curses.A_BOLD)
                    
                    x_offset += col_width
            except curses.error:
                pass
        
        # Enhanced data rows with better highlighting
        for i in range(self.visible_rows):
            row_idx = self.scroll_row + i
            if row_idx >= len(self.buffer.data):
                break
            
            screen_row = 3 + i
            is_current_row = (row_idx == self.cursor_row)
            
            # Row number with highlighting for current row
            row_num_text = f"{row_idx+1:>5} "
            try:
                if is_current_row:
                    stdscr.addstr(screen_row, 0, row_num_text, curses.A_BOLD | curses.A_REVERSE)
                else:
                    stdscr.addstr(screen_row, 0, row_num_text, curses.A_DIM)
            except curses.error:
                pass
            
            # Draw cells with dynamic widths
            x_offset = 6
            for j in range(self.visible_cols):
                col_idx = self.scroll_col + j
                if col_idx >= len(self.buffer.headers):
                    break
                
                col_width = self.buffer.get_column_width(col_idx)
                cell_value = self.buffer.get_cell(row_idx, col_idx)
                
                # Handle multiline content properly - take only first line for display
                lines = cell_value.split('\n')
                first_line = lines[0] if lines else ""
                
                # Extra safety: remove any remaining newlines or special characters
                first_line = first_line.replace('\n', '').replace('\r', '').replace('\t', ' ')
                
                # Truncate cell value if needed (only show first line)
                display_value = first_line[:col_width-1]
                if len(first_line) > col_width-1:
                    display_value = display_value[:-1] + "…"
                
                # Add indicator if cell has multiple lines
                if len(lines) > 1:
                    if len(display_value) < col_width-2:
                        display_value += "↵"  # Add multiline indicator
                    else:
                        display_value = display_value[:-1] + "↵"
                
                # Final safety: ensure no newlines in display text
                display_value = display_value.replace('\n', '').replace('\r', '')
                # Absolutely ensure we don't exceed column width
                display_value = display_value[:col_width-1]
                
                # Determine cell highlighting
                is_current_cell = (row_idx == self.cursor_row and col_idx == self.cursor_col)
                is_selected = (row_idx, col_idx) in self.selected_cells
                is_search_result = (row_idx, col_idx) in self.search_manager.search_results
                
                cell_text = f"{display_value:<{col_width}}"
                # Final sanity check - truncate cell_text to exact column width  
                cell_text = cell_text[:col_width]
                
                try:
                    # Extra safety: ensure we're within screen bounds and table area
                    height, width = stdscr.getmaxyx()
                    max_table_row = 3 + self.visible_rows  # Table ends at this row
                    
                    # Only draw if within proper table bounds
                    if screen_row < max_table_row and screen_row < height - 2 and x_offset < width - col_width:
                        if is_current_cell:
                            # Current cell - bright reverse
                            stdscr.addstr(screen_row, x_offset, cell_text, curses.A_REVERSE | curses.A_BOLD)
                        elif is_selected:
                            # Selected cells
                            stdscr.addstr(screen_row, x_offset, cell_text, curses.A_STANDOUT)
                        elif is_search_result:
                            # Search result highlight
                            stdscr.addstr(screen_row, x_offset, cell_text, curses.A_UNDERLINE)
                        elif is_current_row:
                            # Current row - subtle highlight
                            stdscr.addstr(screen_row, x_offset, cell_text, curses.A_DIM)
                        else:
                            # Normal cell
                            stdscr.addstr(screen_row, x_offset, cell_text)
                except curses.error:
                    pass
                
                x_offset += col_width
        
        # Enhanced help line based on mode
        help_y = height - 2
        if self.mode == 'NORMAL':
            if self.buffer.file_format != 'ccsv':
                help_text = "hjkl:move i:edit :compress(storage) /:search s:sort F1:help q:quit"
            else:
                help_text = "hjkl:move i:edit /:search s:sort F1:help q:quit"
        elif self.mode == 'INSERT':
            help_text = "Enter:save↓ Tab:save→ Shift+Tab:save← Arrows:move Esc:cancel F1:help"
        elif self.mode == 'VISUAL':
            help_text = "hjkl:extend y:copy d:delete c:change r:replace a:all V:line Esc:cancel F1:help"
        elif self.mode == 'COMMAND':
            help_text = "Enter:execute Tab:complete Esc:cancel (w:save q:quit e:open find:search sort:sort)"
        elif self.mode == 'SEARCH':
            help_text = "Enter:search Esc:cancel (then use n:next N:prev to navigate results)"
        else:
            help_text = "F1:help"
        
        try:
            stdscr.addstr(help_y, 0, help_text[:width-1], curses.A_DIM)
        except curses.error:
            pass
        
        # Enhanced status line
        status_y = height - 1
        try:
            stdscr.addstr(status_y, 0, self.status_message[:width-1])
        except curses.error:
            pass
        
        stdscr.refresh()
    
    def run(self, stdscr, file_path: str = None):
        """Enhanced main run loop"""
        # Store stdscr reference for help functionality
        self.stdscr = stdscr
        
        # Initialize curses
        curses.curs_set(0)  # Hide cursor
        stdscr.timeout(100)  # Non-blocking input with timeout
        
        # Load file if provided
        if file_path:
            from pathlib import Path
            file_existed = Path(file_path).exists()
            
            if self.buffer.load_from_file(file_path):
                if not file_existed:
                    self.status_message = f"Created new file {file_path} with default structure"
                else:
                    self.status_message = f"Loaded {file_path} ({len(self.buffer.data)} rows, {len(self.buffer.headers)} cols)"
            else:
                self.status_message = f"Failed to load {file_path}"
        else:
            self.status_message = "Enhanced CSV Editor - F1 for help, :e <file> to open"
        
        # Start auto-save
        self.start_auto_save()
        
        try:
            while True:
                self.draw_screen(stdscr)
                
                key = stdscr.getch()
                if key == -1:  # Timeout
                    continue
                
                # Global shortcuts
                if key == curses.KEY_F1:
                    self.show_help()
                    continue
                elif key == 19:  # Ctrl+S - quick save
                    if self.buffer.stream_save():
                        self.status_message = "Quick saved"
                    else:
                        self.status_message = "Quick save failed"
                    continue
                
                # Handle based on mode
                continue_running = True
                if self.mode == 'NORMAL':
                    continue_running = self.handle_normal_mode(key)
                elif self.mode == 'INSERT':
                    continue_running = self.handle_insert_mode(key)
                elif self.mode == 'FULLSCREEN_EDIT':
                    continue_running = self.handle_fullscreen_edit_mode(key)
                elif self.mode == 'VISUAL':
                    continue_running = self.handle_visual_mode(key)
                elif self.mode == 'COMMAND':
                    continue_running = self.handle_command_mode(key)
                elif self.mode == 'SEARCH':
                    continue_running = self.handle_search_mode(key)
                
                if not continue_running:
                    break
                    
        except KeyboardInterrupt:
            pass
        finally:
            self.auto_save_enabled = False


def main():
    """Main entry point"""
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    if file_path and file_path.endswith(".xlsx"):
        print("Excel file detected - you'll be able to select which sheet to load.")
        print("The selected sheet will be saved as: filename_sheetname.csv")
        print("Use :sheets command to list all sheets, :sheet to switch sheets.")
        print("Press Ctrl+C to cancel, or wait 3 seconds to continue...")
        try:
            time.sleep(3)
            editor = VimCSVEditor()
        except KeyboardInterrupt:
            print("\nLoad cancelled by user")
            sys.exit(0)
    else:
        editor = VimCSVEditor()
    
    try:
        curses.wrapper(editor.run, file_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

# CSVEdit: Enhanced Vim-like Terminal CSV Editor

A lightning-fast, buffer-based CSV editor with vim-like keybindings, powered by curses and designed for efficient data manipulation in the terminal.

## 🚀 Features

### Core Functionality
- **Vim-like Navigation**: Full hjkl movement
- **Multiple Edit Modes**: Normal, Insert, Visual, Command, Search, and Fullscreen Edit
- **Smart Data Handling**: Preserves int/float/string types, auto-detects data types
- **Excel Integration**: Reads XLSX files with sheet selection, saves as CSV
- **Lightning Performance**: Buffer-based with Polars for fast I/O operations

### Advanced Features
- **Undo/Redo System**: Complete operation history with 100-level deep undo
- **Search & Replace**: Regex support with match highlighting and navigation
- **Visual Mode**: Block selection with multi-cell operations
- **Auto-save**: Background saving every 30 seconds
- **Smart Column Management**: Auto-resize, custom widths, insert/delete columns
- **Fullscreen Text Editor**: Scrollable multi-line editing for large content
- **Type Preservation**: Maintains original data types during editing

## 📦 Installation

### Dependencies
```bash
pip install curses polars pandas openpyxl zstandard --break-system-packages
```

### Usage
```bash
python3 csv_edit.py [filename.csv | filename.xlsx | filename.ccsv]
```

If no filename is provided, creates a new CSV with default structure.

## ⌨️ Keyboard Reference

### Normal Mode (Primary Mode)

#### Navigation
| Key | Action | Example |
|-----|--------|---------|
| `h,j,k,l` | Move left/down/up/right |
| `w,b` | Next/previous column |
| `0,$` | Beginning/end of row |
| `^` | First non-empty cell |
| `g,G` | First/last row |
| `H,M,L` | Top/middle/bottom of screen |

#### Page Navigation
| Key | Action |
|-----|--------|
| `Ctrl+f` | Page down |
| `Ctrl+b` | Page up |
| `Ctrl+d` | Half page down |
| `Ctrl+u` | Half page up |

#### Editing
| Key | Action | Description |
|-----|--------|-------------|
| `i` | Insert mode | Edit current cell |
| `a` | Append mode | Same as insert |
| `I` | Fullscreen edit | Multi-line text editor |
| `A` | Goto cell | Dialog to jump to specific cell |
| `R` | Replace mode | Overwrite cell content |
| `r` | Replace char | Replace single character |

#### Row Operations
| Key | Action |
|-----|--------|
| `o` | Insert row below |
| `O` | Insert row above |
| `d` | Delete row |
| `D` | Duplicate row |

#### Column Operations
| Key | Action |
|-----|--------|
| `c` | Insert column right |
| `C` | Insert column left |
| `X` | Delete current column |
| `e` | Edit column name |

#### Copy/Paste
| Key | Action |
|-----|--------|
| `y` | Yank (copy) |
| `p` | Paste |
| `P` | Paste before |

#### Visual Mode
| Key | Action |
|-----|--------|
| `v` | Visual selection |
| `V` | Visual line (entire rows) |

#### Search
| Key | Action |
|-----|--------|
| `/` | Search forward |
| `?` | Search backward |
| `n` | Next match |
| `N` | Previous match |

#### Sorting
| Key | Action |
|-----|--------|
| `s` | Sort column ascending |
| `S` | Sort column descending |

#### Column Width
| Key | Action |
|-----|--------|
| `=` | Auto-resize all columns |
| `+` | Increase column width |
| `-` | Decrease column width |

#### Undo/Redo
| Key | Action |
|-----|--------|
| `u` | Undo |
| `Ctrl+r` | Redo |

#### Misc
| Key | Action |
|-----|--------|
| `:` | Command mode |
| `x` | Clear cell |
| `F1` or `H` | Help |
| `q` | Quit |

### Insert Mode

| Key | Action |
|-----|--------|
| `Esc` | Exit to normal mode |
| `Enter` | Save and move down |
| `Tab` | Save and move right |
| `Shift+Tab` | Save and move left |
| `Arrows` | Navigate cells (when buffer empty) |
| `Backspace` | Delete character |

### Fullscreen Edit Mode (I key)

#### Navigation
| Key | Action |
|-----|--------|
| `Arrows` | Move cursor |
| `Ctrl+h,j,k,l` | Vim-style movement |
| `Ctrl+w,b` | Word movement |
| `Home,End` | Line start/end |
| `Page Up/Down` | Page scrolling |
| `Ctrl+a,e` | Line beginning/end |

#### Horizontal Scrolling
| Key | Action |
|-----|--------|
| `Ctrl+f,d` | Scroll right 10 chars |
| `Ctrl+p,n` | Scroll right/left 20 chars |

#### Line Operations
| Key | Action |
|-----|--------|
| `Enter` | Split line at cursor |
| `Ctrl+r` | Alternative line split |
| `Ctrl+u` | Insert line above |
| `Ctrl+i` | Insert line below |

#### Save/Exit
| Key | Action |
|-----|--------|
| `Ctrl+s` | Save and continue |
| `Ctrl+x` | Save and exit |
| `Esc` | Cancel without saving |

### Visual Mode

| Key | Action |
|-----|--------|
| `Arrows/hjkl` | Extend selection |
| `y` | Yank selection |
| `d` | Delete selection |
| `c` | Change (delete and insert) |
| `r` | Replace all with character |
| `a` | Select all data |
| `Esc` | Exit visual mode |

### Command Mode

| Command | Action |
|---------|--------|
| `:w` | Save file |
| `:w filename` | Save as |
| `:q` | Quit |
| `:wq` | Save and quit |
| `:e filename` | Open file |
| `:new` | New buffer |
| `:goto A1` | Go to cell |
| `:sort column [asc\|desc]` | Sort by column |
| `:find pattern` | Search |
| `:help` | Show help |
| `:compress` | Compress to ccsv |

### Search Mode

| Key | Action |
|-----|--------|
| `Enter` | Execute search |
| `Esc` | Cancel search |
| `Backspace` | Edit pattern |

### Key Features Deep Dive

#### Smart Data Type Handling
```python
# Automatically detects and preserves types
original_types = {
    0: int,    # Column 0 contains integers
    1: float,  # Column 1 contains floats
    2: str     # Column 2 contains strings
}
```

#### Column Width Management
```python
# Auto-resize based on content
buffer.auto_resize_columns()

# Custom width setting
buffer.set_column_width(col_idx, width)
```

## 🔧 Configuration

### Auto-save Settings
```python
auto_save_enabled = True
auto_save_interval = 30  # seconds
```

### Display Settings
```python
show_grid_lines = True
show_row_numbers = True
show_column_letters = True
```

### Undo Settings
```python
max_history = 100  # Maximum undo levels
```

## 🐛 Troubleshooting

### Common Issues

#### Terminal Compatibility
```bash
# Ensure proper terminal support
export TERM=xterm-256color
```

#### Missing Dependencies
```bash
# Install all required packages
pip install curses pyarrow pandas openpyxl --break-system-packages
```

```bash
# Install all required packages (v4)
pip install curses polars pandas openpyxl --break-system-packages
```

#### File Permissions
```bash
# Ensure write permissions
chmod 644 filename
```

### Performance Issues
- Slow scrolling: Reduce column widths
- Memory usage: Compress to ccsv or reduce history limits

## 🔮 Advanced Usage

### Excel Workflow
```bash
# 1. Open Excel file
python3 csv_edit.py data.xlsx

# 2. Select sheet from interactive dialog
# 3. Edit with full vim functionality
# 4. Saves as CSV with preserved types
```

### Complex Searches
```vim
# Case-sensitive search
/Pattern

# Navigate through results
n (next)
N (previous)
```

### Multi-line Cell Editing
```vim
# Enter fullscreen mode
I

# Edit with full text editor capabilities
# - Line numbers
# - Horizontal scrolling
# - Word movement
# - Multiple line operations

# Save and exit
Ctrl+x
```

### Output Format
- **CSV**: Always saves as CSV with proper type preservation
- **UTF-8**: Unicode support for international characters

### Type Preservation
```python
# Original Excel types are preserved:
# - Integers remain integers
# - Floats remain floats
# - Strings remain strings
# - Empty cells handled properly
```

**Quick Reference Card**: Press `F1` or `H` in the editor for interactive help.

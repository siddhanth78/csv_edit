# CSVEdit — Terminal CSV/XLSX Editor

A fast, curses-based **terminal spreadsheet editor** for `.csv` and `.xlsx` files.  
Optimized for large datasets, low memory usage, and productivity in the command line.

---

Run using:
`python3 csv_edit.py <file.csv|file.xlsx>`
or
`csvedit <file.csv|file.xlsx>` after installation (script below)

## Key Bindings

### Navigation
| Key | Action |
|-----|--------|
| ↑ / ↓ / ← / → | Move cursor |
| PgUp / PgDn | Scroll up/down one page |
| Home / End | Jump to first or last column |
| g | Go to specific row number |

---

### Editing
| Key | Action |
|-----|--------|
| e | Edit current cell |
| d | Clear current cell |
| r | Insert new row below |
| R | Insert new row above |
| x | Delete current row |
| C | Insert new column right |
| V | Insert new column left |
| X | Delete current column |

---

### Copy / Paste
| Key | Action |
|-----|--------|
| ; | Start selection mode |
| Enter | Confirm selection |
| c | Copy selected cells |
| v | Paste copied block |
| Esc | Cancel selection |

---

### Search
| Key | Action |
|-----|--------|
| / | Start text search |
| n | Next match |
| N | Previous match |

---

### Popups & Info
| Key | Action |
|-----|--------|
| f | Show full cell content |
| **Shift+F** | Show scrollable popup for long content |
| ? | Show help popup (press again to close) |

---

### File Operations
| Key | Action |
|-----|--------|
| s | Save file |
| :w | Write file |
| :q | Quit |
| :wq | Write and quit |
| q | Quit (press again to force) |

---

## Supported File Types

| Format | Read | Write | Notes |
|---------|------|--------|-------|
| `.csv` | ✅ | ✅ | Fully supported |
| `.xlsx` | ✅ | ⚠️ | Converted internally — some formatting may be lost |

> Unsupported file types trigger a curses popup error:
> ```
> Error: File type not supported (.txt)
> Press any key to exit.
> ```

---

## Dependencies

| Package | Purpose |
|----------|----------|
| `python3` (≥3.8) | Core runtime |
| `curses` | Terminal UI |
| `openpyxl` | XLSX read/write support |
| `csv` | Native CSV streaming |
| `shutil`, `os`, `threading`, `multiprocessing` | File management and concurrency |
| `ProcessPoolExecutor` | Async processing for background writes |

Install them with:
```bash
pip install openpyxl
```

## Installation

```
#!/usr/bin/env bash
# =========================================
# Install script for csvedit terminal tool
# =========================================

set -e

APP_NAME="csvedit"
INSTALL_DIR="/usr/local/bin"
PY_SCRIPT="csv_edit.py"

# Find directory where install script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="$INSTALL_DIR/$PY_SCRIPT"
WRAPPER="$INSTALL_DIR/$APP_NAME"

echo "Installing $APP_NAME ..."

# Verify main file exists
if [[ ! -f "$SCRIPT_DIR/$PY_SCRIPT" ]]; then
  echo "❌ Error: $PY_SCRIPT not found in $SCRIPT_DIR"
  exit 1
fi

# Copy main script
if [[ ! -w "$INSTALL_DIR" ]]; then
  echo "⚠️  Root permission required."
  sudo cp "$SCRIPT_DIR/$PY_SCRIPT" "$TARGET"
  sudo chmod 755 "$TARGET"
else
  cp "$SCRIPT_DIR/$PY_SCRIPT" "$TARGET"
  chmod 755 "$TARGET"
fi

# Create a clean Bash wrapper
WRAPPER_CONTENT="#!/usr/bin/env bash
python3 \"$TARGET\" \"\$@\"
"
echo "$WRAPPER_CONTENT" | sudo tee "$WRAPPER" >/dev/null
sudo chmod +x "$WRAPPER"

echo "✅ Installed successfully!"
echo ""
echo "You can now run:"
echo "    csvedit <file.csv|file.xlsx>"
echo ""

```

## Uninstallation

```
#!/usr/bin/env bash
set -e

APP_NAME="csvedit"
INSTALL_DIR="/usr/local/bin"
PY_SCRIPT="csv_edit.py"

echo "Uninstalling $APP_NAME..."

sudo rm -f "$INSTALL_DIR/$APP_NAME" "$INSTALL_DIR/$PY_SCRIPT"

echo "✅ Uninstalled successfully."

```

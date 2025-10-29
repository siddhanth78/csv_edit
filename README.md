- Lightweight, low memory footprint csv editor
- Supports CSV and XLSX
- macOS based
- Python 3.7+
- Requires pyarrow, pandas, openpyxl for csv/xlsx ops

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

## Uninstall

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

from pathlib import Path
import string
import shutil

# =========================
# Configuration
# =========================

# Date to act on
DATE = "2026-05-15"

# Number of files to create (1-26)
NUM_FILES = 10

# Folder containing the files
FOLDER = Path("logged_runs")

# =========================
# Validation
# =========================

if not (1 <= NUM_FILES <= 26):
    raise ValueError("NUM_FILES must be between 1 and 26.")

# =========================
# File creation
# =========================

letters = string.ascii_lowercase[:NUM_FILES]

# Source file is always the 'a' file
source_file = FOLDER / f"{DATE}a.txt"

if not source_file.exists():
    raise FileNotFoundError(f"Source file does not exist: {source_file}")

for letter in letters:
    target_file = FOLDER / f"{DATE}{letter}.txt"

    # Skip copying onto itself
    if target_file == source_file:
        continue

    shutil.copy(source_file, target_file)
    print(f"Created: {target_file}")

print("Done.")
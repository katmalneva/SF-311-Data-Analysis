# SF 311 Data Analysis

## Setup

This project uses **[uv](https://docs.astral.sh/uv/)** for dependency management and virtual environments.

### 1. Install uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# or with Homebrew
brew install uv
```

After installing, restart your terminal so the `uv` command is available.

### 2. Clone the repo and sync dependencies

```bash
git clone <repo-url>
cd SF-311-Data-Analysis
uv sync
```

`uv sync` will:
- Create a `.venv` virtual environment (if one doesn't exist)
- Install the exact dependencies pinned in `uv.lock`

### 3. Adding a new dependency

```bash
uv add <package-name>
```

This updates both `pyproject.toml` and `uv.lock`. Commit both files so the rest of the team stays in sync.

### 4. Running scripts

```bash
uv run python main.py
```

`uv run` ensures the script executes inside the project's virtual environment.

---

## MongoDB Connection

The shared cluster is **msds697-dds-groupproject8-cluster** on MongoDB Atlas.

Connection string:

```
mongodb+srv://ttran48_db_user:<db_password>@msds697-dds-groupprojec.0ftcbw7.mongodb.net/?appName=msds697-dds-groupproject8-cluster
```

Replace `<db_password>` with the database user password (ask the team if you don't have it).

### Connecting with pymongo

First add the dependency:

```bash
uv add "pymongo[srv]"
```

Then in Python:

```python
from pymongo import MongoClient

client = MongoClient(
    "mongodb+srv://ttran48_db_user:<db_password>@msds697-dds-groupprojec.0ftcbw7.mongodb.net/?appName=msds697-dds-groupproject8-cluster"
)

db = client["<database_name>"]
```

## About the Data Set

[Link](https://data.sfgov.org/City-Infrastructure/311-Cases/vw6y-z8j6/about_data) to data.

---

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

### 3. Configure environment variables

Copy the provided example file and fill in any secrets or machine-specific paths:

```bash
cp .env.example .env
```

Update `.env` with your MongoDB URI, GCP bucket, Socrata token, etc., before running any services. Never commit your populated `.env`.

### 4. Adding a new dependency

```bash
uv add <package-name>
```

This updates both `pyproject.toml` and `uv.lock`. Commit both files so the rest of the team stays in sync.

### 5. Running scripts

```bash
uv run python main.py
```

`uv run` ensures the script executes inside the project's virtual environment.

## About the Data Set

[Link](https://data.sfgov.org/City-Infrastructure/311-Cases/vw6y-z8j6/about_data) to data.

---

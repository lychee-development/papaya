# Papaya 🌺

Papaya is a utility that automatically creates and runs a [FastMCP](https://github.com/fastmcp/fastmcp) server alongside your FastAPI application.

## Why Papaya?

Papaya makes your FastAPI endpoints instantly available as tools through the FastMCP protocol with zero configuration - just run your FastAPI app with Papaya, and it automatically creates and manages a FastMCP server for you.

## Installation

```bash
pip install papaya
```

## Usage

### Command Line

The simplest way to use Papaya is through its command-line interface:

```bash
papaya uvicorn your_module:app --port 8000 --reload
```

This will:
1. Start your FastAPI app on port 8000 (with hot reloading)
2. Automatically create and run a FastMCP server on port 8001 (default)

You can customize the FastMCP server with additional options:

```bash
papaya uvicorn your_module:app --mcp-port 9000 --port 8000 --reload
```

## Configuration Options

Papaya can be configured with the following options:

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--mcp-host` | Host for the FastMCP server | "0.0.0.0" |
| `--mcp-port` | Port for the FastMCP server | 8001 |
| `--max-unwrap-depth` | Maximum depth to traverse middleware stack | 10 |
| `--no-verbose` | Disable verbose logging from Papaya | False |

Any additional options are passed directly to Uvicorn.

## Requirements

- Python 3.7+
- FastAPI
- Uvicorn
- FastMCP

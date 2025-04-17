import sys
import uvicorn
import main
from main import papaya_print, PAPAYA_COLOR, RESET_COLOR

from fastapi_wrapper import wrap_fastapi
wrap_fastapi()

def main_cli():
    """Entry point for the papaya command line tool"""
    if len(sys.argv) < 2 or sys.argv[1] != "uvicorn":
        print("Usage: papaya uvicorn <uvicorn_arguments...>")
        print("Example: papaya uvicorn your_module:app --host 0.0.0.0 --port 8000 --reload")
        print("\nPapaya specific options:")
        print("  --mcp-host HOST      Host for the FastMCP server (default: 0.0.0.0)")
        print("  --mcp-port PORT      Port for the FastMCP server (default: 8001)")
        sys.exit(1)

    # Extract papaya-specific arguments and configure
    args = sys.argv[1:]  # Remove 'papaya' command
    i = 0

    # setting defaults
    MCP_PORT_GLOBAL = "8000"
    MCP_HOST_GLOBAL = "0.0.0.0"


    while i < len(args):
        if args[i] == "--mcp-host" and i + 1 < len(args):
            MCP_HOST_GLOBAL = args[i + 1]
            # Remove these arguments from the list
            args.pop(i)
            args.pop(i)
        elif args[i] == "--mcp-port" and i + 1 < len(args):
            try:
                MCP_PORT_GLOBAL = int(args[i + 1])
            except ValueError:
                print(f"{PAPAYA_COLOR}PAPAYA:{RESET_COLOR} Error - mcp-port must be an integer, got '{args[i + 1]}'")
                sys.exit(1)
            # Remove these arguments from the list
            args.pop(i)
            args.pop(i)
        else:
            i += 1

    # Check for port conflict (basic check)
    mcp_port = MCP_PORT_GLOBAL
    if f"--port {mcp_port}" in " ".join(args):
        print(f"{PAPAYA_COLOR}Warning:{RESET_COLOR} Detected potential port conflict. The main app might be configured to use port {mcp_port}, which Papaya intends to use for the MCP server.")

    # Remove the 'uvicorn' command from args
    if args and args[0] == "uvicorn":
        args = args[1:]

    try:
        papaya_print(f"Executing Uvicorn with args: {args}")
        uvicorn.main.main(args=args)
    except SystemExit as e:
        pass # Normal exit for Uvicorn CLI
    except Exception as e:
        papaya_print(f"Error during uvicorn execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main_cli()

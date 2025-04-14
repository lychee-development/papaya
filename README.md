# Papaya 🍈

Papaya is a MCP server to enable live observability for python webapps.

## Requirements

- Requires adding one command (papaya ....) before the regular run command 

- Logs major events (requests in/out of server, functions that run for a long time, exceptions that are handled) and retains information about them until some storage/time threshold is met.

- Wraps the webserver and adds required endpoints to also make it an MCP server, that can be accessed via MCP client using a symmetric key

- Easily installable via pip

- Enables security via symmetric key encryption

## Example Usecase 

User has a python webserver that exposes an API for their platform. It allows their customers to login, update information in their account (stored on the backend in a postgres database), and use an ML model to predict future trends based on their historical information.

The regular run command is 
'''uvicorn main:app --host 0.0.0.0 --port 80'''

Instead of running this, to enable Papaya's MCP-integrated observability functionality, they would run
'''papaya uvicorn main:app --host 0.0.0.0 --port 80'''

Assume that the user's webserver is hosted at "userapp.com". Papaya would automatically wrap their webserver, and the user could use cursor any other MCP client to connect to Papaya's observability functionality by connecting to an MCP server hosted at userapp.com/papaya_mcp.

Papaya exposes information like handled exceptions, function calls that took longer than a certain amount of time, failed requests, and suspicious activity, providing information to an MCP client that might be helpful for monitoring or diagnosing issues within the webserver.



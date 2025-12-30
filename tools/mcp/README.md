\# Quant\_System MCP



\## 安装（独立 venv，避免污染主工程依赖）

py -3.12 -m venv .venv\_mcp

.\\.venv\_mcp\\Scripts\\python.exe -m pip install -U pip

.\\.venv\_mcp\\Scripts\\python.exe -m pip install -r tools\\mcp\\requirements-mcp.txt



\## 运行（HTTP）

.\\run\_mcp\_server.bat



\## 停止

CTRL+C（出现 CancelledError/KeyboardInterrupt 属正常退出）




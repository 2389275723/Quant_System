import asyncio
import json
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main():
    # 关键点：用 stdio 方式启动 server 子进程，这样不依赖 8000 端口
    server = StdioServerParameters(
        command=sys.executable,
        args=[r"tools/mcp/quant_server.py"],
    )

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools_resp = await session.list_tools()
            tools = getattr(tools_resp, "tools", [])

            print("\n=== MCP TOOLS ===")
            for t in tools:
                print(f"- {t.name}: {t.description or ''}")
                schema = getattr(t, "inputSchema", None)
                if schema:
                    print(json.dumps(schema, ensure_ascii=False, indent=2))

            # 尝试调用一个常见的健康检查工具（如果你有实现）
            for name in ("ping", "health", "version"):
                if any(tt.name == name for tt in tools):
                    print(f"\n=== CALL {name} ===")
                    r = await session.call_tool(name, {})
                    print(r)
                    break


if __name__ == "__main__":
    asyncio.run(main())

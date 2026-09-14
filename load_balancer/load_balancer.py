import subprocess

from fastapi import FastAPI, Request, Response
import httpx

servers = []
index = 0

client = httpx.AsyncClient(
    limits=httpx.Limits(
        max_connections=10000,
        max_keepalive_connections=10000,
    ),
    timeout=httpx.Timeout(
        connect=10.0,
        read=None,
        write=10.0,
        pool=10.0,
    ),
)

def get_servers():
    result = subprocess.run(
        ["getent", "hosts", "server"],
        capture_output=True,
        text=True,
    )

    for line in result.stdout.splitlines():
        servers.append(f"http://{line.split()[0]}:8001")


load = FastAPI()


get_servers()

print("[LOAD] servers:", servers, flush=True)


@load.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
)
async def forward(path: str, request: Request):
    global index

    backend = servers[index]
    index = (index + 1) % len(servers)

    response = await client.request(
        request.method,
        f"{backend}/{path}",
        params=request.query_params,
        headers=request.headers,
        content=await request.body(),
    )

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response.headers,
    )
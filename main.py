from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import os
import json
import httpx

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
AGENT_ID = os.environ.get("AGENT_ID", "agent_011CaY1QbMWkL8UepiQgHedg")
ENVIRONMENT_ID = os.environ.get("ENVIRONMENT_ID", "env_01CSojXza8dTzSDqdtaUUfhH")

class AuditRequest(BaseModel):
    url: str
    email: str = ""
    keyword: str = ""

async def stream_agent_response(url: str, email: str, keyword: str):
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "anthropic-beta": "managed-agents-2026-04-01",
        "content-type": "application/json"
    }

    message = url
    if keyword:
        message += f" | Mot-clé cible : {keyword}"
    if email:
        message += f" | Envoie le rapport complet par email à : {email}"

    async with httpx.AsyncClient(timeout=30) as client:
        session_resp = await client.post(
            "https://api.anthropic.com/v1/sessions",
            headers=headers,
            json={
                "environment_id": ENVIRONMENT_ID,
                "agent": {"type": "agent", "id": AGENT_ID}
            }
        )
        session = session_resp.json()
        session_id = session.get("id")

    if not session_id:
        yield f"data: {json.dumps({'type': 'error', 'content': 'Impossible de créer la session'})}\n\n"
        return

    yield f"data: {json.dumps({'type': 'status', 'content': 'Session démarrée...'})}\n\n"

    async with httpx.AsyncClient(timeout=300) as client:
        async with client.stream(
            "POST",
            f"https://api.anthropic.com/v1/sessions/{session_id}/events",
            headers=headers,
            json={"events": [{"type": "user", "content": [{"type": "text", "text": message}]}]}
        ) as response:
            async for line in response.aiter_lines():
                if line.startswith("data:"):
                    try:
                        data = json.loads(line[5:].strip())
                        event_type = data.get("type", "")

                        if event_type == "agent.message":
                            content = data.get("content", [])
                            for block in content:
                                if block.get("type") == "text":
                                    yield f"data: {json.dumps({'type': 'message', 'content': block['text']})}\n\n"

                        elif event_type == "agent.mcp_tool_use":
                            tool_name = data.get("name", "")
                            yield f"data: {json.dumps({'type': 'tool', 'content': f'Utilisation de : {tool_name}'})}\n\n"

                        elif event_type == "session.status_idle":
                            yield f"data: {json.dumps({'type': 'done', 'content': 'Analyse terminée'})}\n\n"

                    except:
                        pass

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/audit")
async def audit(request: AuditRequest):
    return StreamingResponse(
        stream_agent_response(request.url, request.email, request.keyword),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_ID}

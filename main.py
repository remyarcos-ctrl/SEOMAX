from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import os
import json
import httpx
import asyncio

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

BASE_HEADERS = {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "managed-agents-2026-04-01",
    "content-type": "application/json"
}

class AuditRequest(BaseModel):
    url: str
    email: str = ""
    keyword: str = ""

async def stream_agent_response(url: str, email: str, keyword: str):
    message = url
    if keyword:
        message += f" | Mot-clé cible : {keyword}"
    if email:
        message += f" | Envoie le rapport complet par email à : {email}"

    # 1. Créer la session
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/sessions",
                headers=BASE_HEADERS,
                json={
                    "environment_id": ENVIRONMENT_ID,
                    "agent": {"type": "agent", "id": AGENT_ID}
                }
            )
            session_id = r.json().get("id")
            print(f"Session: {session_id}", flush=True)
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
        return

    if not session_id:
        yield f"data: {json.dumps({'type': 'error', 'content': 'Pas de session'})}\n\n"
        return

    yield f"data: {json.dumps({'type': 'status', 'content': 'Connexion à Brandon...'})}\n\n"

    # 2. Envoyer le message utilisateur
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"https://api.anthropic.com/v1/sessions/{session_id}/events",
                headers=BASE_HEADERS,
                json={
                    "events": [
                        {
                            "type": "user.message",
                            "content": [{"type": "text", "text": message}]
                        }
                    ]
                }
            )
            print(f"Message envoyé: {r.status_code}", flush=True)
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"
        return

    # 3. Streamer les événements via GET /stream
    stream_headers = {**BASE_HEADERS, "Accept": "text/event-stream"}
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            async with client.stream(
                "GET",
                f"https://api.anthropic.com/v1/sessions/{session_id}/stream",
                headers=stream_headers,
                params={"beta": "true"}
            ) as response:
                print(f"Stream GET status: {response.status_code}", flush=True)
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        data = json.loads(raw)
                        event_type = data.get("type", "")
                        print(f"Event: {event_type}", flush=True)

                        if event_type == "agent.message":
                            for block in data.get("content", []):
                                if block.get("type") == "text" and block.get("text"):
                                    yield f"data: {json.dumps({'type': 'message', 'content': block['text']})}\n\n"

                        elif event_type == "agent.mcp_tool_use":
                            yield f"data: {json.dumps({'type': 'tool', 'content': f'Utilisation de : {data.get(\"name\", \"\")}'})}\n\n"

                        elif event_type == "session.status_idle":
                            stop = data.get("stop_reason", {})
                            if stop.get("type") == "end_turn":
                                yield f"data: {json.dumps({'type': 'done', 'content': 'Terminé'})}\n\n"
                                break

                    except json.JSONDecodeError:
                        pass

    except Exception as e:
        print(f"Erreur stream: {e}", flush=True)
        yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"


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

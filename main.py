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

BASE_HEADERS = {
    "x-api-key": ANTHROPIC_API_KEY,
    "anthropic-version": "2023-06-01",
    "anthropic-beta": "managed-agents-2026-04-01",
    "content-type": "application/json",
}


class AuditRequest(BaseModel):
    url: str
    email: str = ""
    keyword: str = ""


def build_user_message(url: str, email: str, keyword: str) -> str:
    """Construit le message envoyé à Brandon."""
    msg = f"Audite ce site : {url}"
    if keyword:
        msg += f"\nMot-clé cible : {keyword}"
    if email:
        msg += f"\nEnvoie le rapport final à : {email}"
    return msg


async def stream_agent_response(url: str, email: str, keyword: str):
    """
    Crée une session ET stream les événements en une seule requête SSE.
    C'est ça la bonne méthode — pas de POST /sessions puis GET /stream séparés.
    """
    user_message = build_user_message(url, email, keyword)

    payload = {
        "agent_id": AGENT_ID,
        "environment_id": ENVIRONMENT_ID,
        "input": [
            {
                "type": "user_message",
                "content": [
                    {"type": "text", "text": user_message}
                ],
            }
        ],
        "stream": True,
    }

    try:
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream(
                "POST",
                "https://api.anthropic.com/v1/sessions",
                headers=BASE_HEADERS,
                json=payload,
            ) as response:

                if response.status_code != 200:
                    error_body = await response.aread()
                    yield f"data: {json.dumps({'type': 'error', 'content': f'API {response.status_code}: {error_body.decode()}'})}\n\n"
                    return

                # Parse les événements SSE
                async for line in response.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue

                    data_str = line[5:].strip()
                    if not data_str:
                        continue

                    try:
                        event = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    event_type = event.get("type", "")

                    # Texte généré par l'agent
                    if event_type == "content_block_delta":
                        delta = event.get("delta", {})
                        if delta.get("type") == "text_delta":
                            text = delta.get("text", "")
                            if text:
                                yield f"data: {json.dumps({'type': 'text', 'content': text})}\n\n"

                    # Outil utilisé (web_search, bash, gmail, etc.)
                    elif event_type == "content_block_start":
                        block = event.get("content_block", {})
                        if block.get("type") in ("tool_use", "server_tool_use", "mcp_tool_use"):
                            tool_name = block.get("name", "outil")
                            yield f"data: {json.dumps({'type': 'tool', 'content': f'🔧 Utilisation de : {tool_name}'})}\n\n"

                    # Session terminée
                    elif event_type == "session.status_idle":
                        yield f"data: {json.dumps({'type': 'done', 'content': 'Terminé'})}\n\n"
                        return

                    # Erreur côté agent
                    elif event_type == "error":
                        err = event.get("error", {})
                        yield f"data: {json.dumps({'type': 'error', 'content': err.get('message', 'Erreur inconnue')})}\n\n"
                        return

    except httpx.HTTPError as e:
        yield f"data: {json.dumps({'type': 'error', 'content': f'Erreur HTTP : {str(e)}'})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': f'Erreur : {str(e)}'})}\n\n"


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
            "Access-Control-Allow-Origin": "*",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_ID, "env": ENVIRONMENT_ID}

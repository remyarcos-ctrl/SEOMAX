from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
import os
import json
import anthropic

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
    message = url
    if keyword:
        message += f" | Mot-clé cible : {keyword}"
    if email:
        message += f" | Envoie le rapport complet par email à : {email}"

    yield f"data: {json.dumps({'type': 'status', 'content': 'Connexion à Brandon...'})}\n\n"

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        # Créer la session
        session = client.beta.sessions.create(
            environment_id=ENVIRONMENT_ID,
            agent={"type": "agent", "id": AGENT_ID},
            betas=["managed-agents-2026-04-01"]
        )
        session_id = session.id
        print(f"Session: {session_id}", flush=True)

        # Envoyer le message et streamer
        with client.beta.sessions.events.stream(
            session_id=session_id,
            events=[{
                "type": "user.message",
                "content": [{"type": "text", "text": message}]
            }],
            betas=["managed-agents-2026-04-01"]
        ) as stream:
            for event in stream:
                event_type = getattr(event, "type", "")
                print(f"Event: {event_type}", flush=True)

                if event_type == "agent.message":
                    content = getattr(event, "content", [])
                    for block in content:
                        if getattr(block, "type", "") == "text":
                            yield f"data: {json.dumps({'type': 'message', 'content': block.text})}\n\n"

                elif event_type == "agent.mcp_tool_use":
                    yield f"data: {json.dumps({'type': 'tool', 'content': f'Utilisation de : {getattr(event, \"name\", \"\")}'})}\n\n"

                elif event_type == "session.status_idle":
                    yield f"data: {json.dumps({'type': 'done', 'content': 'Terminé'})}\n\n"
                    break

    except Exception as e:
        print(f"Erreur: {e}", flush=True)
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

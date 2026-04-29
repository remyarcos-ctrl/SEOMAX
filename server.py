from fastmcp import FastMCP
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
import base64
import os
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

mcp = FastMCP("Gmail MCP pour SEOMAX")

# Credentials depuis variables d'environnement
def get_gmail_service():
    creds = Credentials(
        token=os.environ.get("GMAIL_ACCESS_TOKEN"),
        refresh_token=os.environ.get("GMAIL_REFRESH_TOKEN"),
        client_id=os.environ.get("GMAIL_CLIENT_ID"),
        client_secret=os.environ.get("GMAIL_CLIENT_SECRET"),
        token_uri="https://oauth2.googleapis.com/token"
    )
    return build("gmail", "v1", credentials=creds)

@mcp.tool()
def send_email(to: str, subject: str, body: str, html: bool = False) -> str:
    """Envoie un email via Gmail"""
    service = get_gmail_service()
    
    message = MIMEMultipart("alternative")
    message["to"] = to
    message["subject"] = subject
    
    if html:
        message.attach(MIMEText(body, "html"))
    else:
        message.attach(MIMEText(body, "plain"))
    
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    result = service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()
    
    return f"Email envoyé avec succès ! ID: {result['id']}"

@mcp.tool()
def list_emails(query: str = "", max_results: int = 10) -> str:
    """Liste les emails selon une requête"""
    service = get_gmail_service()
    results = service.users().messages().list(
        userId="me", q=query, maxResults=max_results
    ).execute()
    
    messages = results.get("messages", [])
    if not messages:
        return "Aucun email trouvé"
    
    emails = []
    for msg in messages[:5]:
        detail = service.users().messages().get(
            userId="me", id=msg["id"], format="metadata",
            metadataHeaders=["From", "Subject", "Date"]
        ).execute()
        headers = {h["name"]: h["value"] for h in detail["payload"]["headers"]}
        emails.append(f"- {headers.get('Subject','(sans objet)')} | De: {headers.get('From','')} | {headers.get('Date','')}")
    
    return "\n".join(emails)

@mcp.tool()
def read_email(message_id: str) -> str:
    """Lit le contenu d'un email par son ID"""
    service = get_gmail_service()
    msg = service.users().messages().get(
        userId="me", id=message_id, format="full"
    ).execute()
    
    headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
    
    body = ""
    if "parts" in msg["payload"]:
        for part in msg["payload"]["parts"]:
            if part["mimeType"] == "text/plain":
                body = base64.urlsafe_b64decode(part["body"]["data"]).decode()
                break
    elif "body" in msg["payload"] and "data" in msg["payload"]["body"]:
        body = base64.urlsafe_b64decode(msg["payload"]["body"]["data"]).decode()
    
    return f"De: {headers.get('From','')}\nObjet: {headers.get('Subject','')}\nDate: {headers.get('Date','')}\n\n{body[:2000]}"

if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

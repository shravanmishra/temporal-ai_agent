import uvicorn
from dotenv import load_dotenv

load_dotenv()

def run_services():
    print("🚀 Initializing Uvicorn Server Layer on http://localhost:8000")
    uvicorn.run("temporal_agent.api:app", host="0.0.0.0", port=8000, log_level="info")

if __name__ == "__main__":
    run_services()

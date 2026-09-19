import uvicorn
from dotenv import load_dotenv

load_dotenv()

def run_services():
    """Main launcher entrypoint that triggers the web application server thread safely."""
    print("🚀 Initializing Uvicorn Server Layer on http://localhost:8000")
    # We reference the api file string path so uvicorn instantiates the async loop safely
    uvicorn.run("temporal_agent.api:app", host="0.0.0.0", port=8000, log_level="info")

if __name__ == "__main__":
    run_services()

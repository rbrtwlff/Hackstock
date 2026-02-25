from app.main import app, config
import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host=config.host, port=config.port)

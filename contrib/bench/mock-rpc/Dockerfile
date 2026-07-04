# JSON-RPC mock that replays the frozen dataset. Stdlib only, so no dependencies.
# The dataset is bind-mounted at /dataset at run time.
FROM python:3-slim
COPY server.py /app/server.py
EXPOSE 8232
ENTRYPOINT ["python3", "/app/server.py"]

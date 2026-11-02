FROM pytorch/pytorch:2.5.1-cuda12.4-cudnn9-devel
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN uv pip install --system --no-cache dagster-pipes==1.13.21 boto3==1.43.91
COPY worker.py /app/worker.py
ENTRYPOINT ["python", "/app/worker.py"]

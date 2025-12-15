FROM python:3.12-slim
COPY portal/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
COPY portal/app.py portal/models.yaml /app/
COPY portal/static /app/static
WORKDIR /app
USER 65534
ENTRYPOINT ["python", "/app/app.py"]

FROM python:3.12-slim
COPY gateway/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
COPY gateway/app.py /app/app.py
WORKDIR /app
ENTRYPOINT ["python", "/app/app.py"]

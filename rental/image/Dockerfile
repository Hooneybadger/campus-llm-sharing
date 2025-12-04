FROM python:3.12-slim
COPY gateway/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
COPY rental/manager.py rental/autoscale.py /app/
ENTRYPOINT ["python", "/app/manager.py"]

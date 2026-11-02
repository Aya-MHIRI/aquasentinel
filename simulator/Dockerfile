FROM python:3.11-slim
WORKDIR /app/simulator
RUN pip install --no-cache-dir confluent-kafka==2.5.0
COPY sim_model.py simulator.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "simulator.py"]

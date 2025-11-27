FROM python:3.12-slim

# Reduce Python noise and bytecode writes
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# System deps for building some Python wheels (httplib2, google libs)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Run migrations on container start, then launch Gunicorn
CMD ["sh", "-c", "python manage.py migrate && gunicorn web_project.wsgi:application --bind 0.0.0.0:8000 --workers 3 --timeout 120"]

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpoppler-cpp-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Parse PDF and generate questions.json at build time
# RUN python parse_pdf.py test.pdf > questions.json

CMD ["python", "bot.py"]

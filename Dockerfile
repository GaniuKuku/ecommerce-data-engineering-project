# Starting with a lightweight Python computer
FROM python:3.11-slim

# Creates a working folder to put all my code in
WORKDIR /app

# Copy my requirements list and install the tools
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all my actual pipeline code into the computer
COPY . .

# Tell it where to find my Google Cloud key so it can log in
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/gcp-key.json"

# What should my computer do when it turns on?
CMD ["python", "orchestrate.py"]
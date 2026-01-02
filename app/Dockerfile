# Python runtime
FROM python:3.9-slim

# Working directory 
WORKDIR /app

# Requirements
COPY requirements.txt .

# Install packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Instala las dependencias necesarias para pygame
RUN apt-get update && apt-get install -y libasound2 libsdl2-mixer-2.0-0 libglib2.0-0 pulseaudio alsa-utils && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Expose the port that Flask will run on
EXPOSE 3000

# Run the application
CMD ["python", "asistente-worker.py"]

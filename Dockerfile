# Use an official Python runtime as a parent image
# The -slim version is smaller and more secure
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /code

# Install any dependencies specified in requirements.txt
# We copy this first to leverage Docker layer caching.
# If requirements.txt doesn't change, this step is cached.
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the entire FastAPI application to the working directory
COPY . /code/app

# Expose the port that the application will run on
EXPOSE 80

# Set the command to run the application using Uvicorn
# 'app.main:app' refers to the 'app' instance inside 'main.py' located in the 'app' directory
# The --host 0.0.0.0 flag is crucial to make the app accessible from outside the container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]

# Use the base Spark image
FROM bitnamilegacy/spark:3.5

# Switch to root user so we have permission to install things
USER root

# Copy the requirements file into the container
COPY requirements.txt /app/requirements.txt

# Install the Python libraries
RUN pip install --no-cache-dir -r /app/requirements.txt

# Switch back to the default non-root user (good practice)
USER 1001
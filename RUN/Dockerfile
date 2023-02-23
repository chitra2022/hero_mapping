# Import Python runtime and set up working directory
FROM python:3.9.5-slim

# Copy local code to the container image.
WORKDIR /app
COPY . .

# Open port 8080 for serving the webpage
EXPOSE 8080

# this is to enable https
RUN apt-get update && apt-get install -y curl gnupg g++
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

# Install any necessary dependencies
# install msodbcsql17
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN exit

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN apt-get install -y unixodbc-dev
RUN sed -i 's/TLSv1.0/TLSv1.2/g' /etc/ssl/openssl.cnf
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf
RUN pip3 install -r requirements.txt 
RUN pip3 install Flask gunicorn

# Run initiate_integration.py when the container launches
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 0 initiate_integration:app

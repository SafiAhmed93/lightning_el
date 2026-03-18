FROM condaforge/miniforge3:latest

# Install Microsoft SQL Server ODBC repository and install msodbcsql18
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    apt-transport-https \
    unixodbc \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Pre-add Microsoft SQL Server tools to PATH
ENV PATH="$PATH:/opt/mssql-tools18/bin"

WORKDIR /app

# Install Python dependencies using conda to get pre-built binaries for turbodbc and pyarrow
RUN mamba install -y -c conda-forge \
    python=3.11 \
    pandas \
    pyarrow \
    sqlalchemy \
    turbodbc \
    adbc-driver-postgresql \
    psycopg2 \
    && mamba clean --all -f -y

COPY . /app

CMD ["tail", "-f", "/dev/null"]

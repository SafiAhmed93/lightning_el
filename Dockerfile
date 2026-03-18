FROM --platform=linux/amd64 condaforge/miniforge3:latest

# Install system dependencies, MS SQL Server ODBC repo, and unixODBC
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

# Install SAP HANA ODBC Driver (hdbclient)
# Requires downloading the public developer edition client tarball (x86_64 only)
RUN mkdir -p /tmp/hana && \
    curl -L -H "Cookie: eula_3_2_agreed=tools.hana.ondemand.com/developer-license-3_2.txt" -o /tmp/hana/hanaclient.tar.gz "https://tools.hana.ondemand.com/additional/hanaclient-latest-linux-x64.tar.gz" && \
    cd /tmp/hana && \
    tar -xzf hanaclient.tar.gz && \
    ./client/hdbinst -a client --batch --path=/opt/sap/hdbclient && \
    echo "[HDBODBC]\nDescription = SAP HANA ODBC Driver\nDriver = /opt/sap/hdbclient/libodbcHDB.so\n" >> /etc/odbcinst.ini && \
    rm -rf /tmp/hana

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

FROM postgres:16

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    postgresql-server-dev-16 \
  && git clone https://github.com/fboulnois/pg_uuidv7 /tmp/pg_uuidv7 \
  && cd /tmp/pg_uuidv7 && make && make install \
  && apt-get remove -y build-essential git postgresql-server-dev-16 \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/* /tmp/pg_uuidv7

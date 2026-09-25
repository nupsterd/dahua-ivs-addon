ARG BUILD_FROM
FROM $BUILD_FROM

LABEL maintainer="nupsterd"
LABEL description="Dahua IVS (tripwire) event consumer for Home Assistant (Portería Virtual)"

# Zona horaria del container: received_ts sale con el offset local de la Pi.
ENV TZ=America/Bogota

# python3 trae sqlite3 (depende de sqlite-libs); tzdata da las zonas a zoneinfo.
RUN apk add --no-cache \
    python3 \
    py3-requests \
    tzdata && \
    cp /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

WORKDIR /app
COPY dahua_ivs/ /app/dahua_ivs/

CMD ["python3", "-m", "dahua_ivs.main"]

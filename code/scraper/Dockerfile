FROM fedora:latest

RUN dnf install -y firefox python3-selenium python3-boto3

RUN curl -L https://github.com/mozilla/geckodriver/releases/download/v0.33.0/geckodriver-v0.33.0-linux64.tar.gz | tar -xz geckodriver -O >/usr/local/bin/geckodriver &&\
    echo '63ec2c608e09e178085a7f618dbcf688ae102abf /usr/local/bin/geckodriver' | sha1sum -c &&\
    chmod +x /usr/local/bin/geckodriver

ENV AWS_DEFAULT_REGION=eu-central-1 PYTHONUNBUFFERED=true

COPY scrape.py /app/
COPY lib/ /app/lib

CMD ["/app/scrape.py"]

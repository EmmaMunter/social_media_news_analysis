FROM fedora:39

RUN dnf update -y &&\
    dnf install -y tigervnc tigervnc-server firefox openbox python3-selenium python3-boto3 &&\
    dnf clean all

RUN curl -L 'https://dist.torproject.org/torbrowser/12.5.3/geckodriver-linux64.tar.xz' -o /geckodriver-linux64.tar.xz &&\
    echo 'e631a0631a4e8095acc1d731567bffa5db499b44  /geckodriver-linux64.tar.xz' | sha1sum -c &&\
    tar -xf /geckodriver-linux64.tar.xz -C /usr/local/bin &&\
    chmod go+rx /usr/local/bin/geckodriver &&\
    rm /geckodriver-linux64.tar.xz


RUN useradd browser-user

USER browser-user

WORKDIR /home/browser-user

RUN curl -L 'https://www.torproject.org/dist/torbrowser/12.5.3/tor-browser-linux64-12.5.3_ALL.tar.xz' -o tor-browser-linux64.tar.xz &&\
    echo '8ca1ad3a07f41f377c479833a07315f68924ff15  tor-browser-linux64.tar.xz' | sha1sum -c &&\
    tar -xf tor-browser-linux64.tar.xz &&\
    rm tor-browser-linux64.tar.xz

ENV AWS_DEFAULT_REGION=eu-central-1 PYTHONUNBUFFERED=true

# COPY --chown=browser-user:browser-user telegram_1.profile.default/ /home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/

# COPY --chown=browser-user:browser-user "telegram_1.profile.default/SiteSecurityServiceState.txt" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/"
# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage.sqlite" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/"
# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/"

# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/"

# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/"

# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/.metadata-v2" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/"
# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/idb/" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/idb/"
# COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/ls/" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/ls/"

# TODO: create the necessary directories, but rely on bind-mounting or volume-mounting of data.sqlite
COPY --chown=browser-user:browser-user "telegram_1.profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/ls/data.sqlite" "/home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/storage/default/https+++web.telegram.org^firstPartyDomain=telegram.org/ls/"

COPY --chown=browser-user:browser-user policies.json /home/browser-user/tor-browser/Browser/distribution/
COPY --chown=browser-user:browser-user user.js /home/browser-user/tor-browser/Browser/TorBrowser/Data/Browser/profile.default/
COPY scrape.py /app/
COPY lib/ /app/lib

CMD ["/app/scrape.py"]
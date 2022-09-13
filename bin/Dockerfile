FROM debian:11

RUN echo "deb http://deb.debian.org/debian bullseye-backports main contrib non-free" >> /etc/apt/sources.list \
    && echo "deb-src http://deb.debian.org/debian bullseye-backports main contrib non-free" >> /etc/apt/sources.list \
    && apt-get update -yq \
    && apt-get install -yqq \
        git \
        git-filter-repo \
        vim \
        openssh-client \
        python3-pip \
    && mkdir -p ~/.ssh || true \
    && chmod 700 ~/.ssh/ \
    && ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts \
    && pip install packaging

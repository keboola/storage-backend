FROM debian:12

RUN echo "deb http://deb.debian.org/debian bookworm-backports main contrib non-free" >> /etc/apt/sources.list \
    && echo "deb-src http://deb.debian.org/debian bookworm-backports main contrib non-free" >> /etc/apt/sources.list \
    && apt-get update -yq \
    && apt-get install -yqq \
        git \
        git-filter-repo \
        vim \
        openssh-client \
        python3-pip \
        python3-packaging \
    && mkdir -p ~/.ssh || true \
    && chmod 700 ~/.ssh/ \
    && ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts

RUN git config --system --add safe.directory /monorepo

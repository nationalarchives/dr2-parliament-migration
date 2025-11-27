FROM eclipse-temurin:21-jre

WORKDIR /app

COPY build.sc Main.scala
RUN curl -sSL https://github.com/VirtusLab/scala-cli/releases/latest/download/scala-cli-x86_64-pc-linux.gz | gzip -d > /usr/bin/scala-cli && \
    chmod +x /usr/bin/scala-cli
RUN mkdir /content && scala-cli compile .
CMD ["scala-cli", "run", "."]

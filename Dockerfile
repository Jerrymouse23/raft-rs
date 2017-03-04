FROM ubuntu:latest 

#ENV RUST_BACKTRACE=1
#ENV RUST_LOG=raft=debug

RUN mkdir /code

VOLUME /config
VOLUME /data

WORKDIR /code
COPY src/document/target/debug/document .

EXPOSE 9000
EXPOSE 3000

CMD ["/code/document","server","/config/config.toml"]

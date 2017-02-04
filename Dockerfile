FROM fnichol/rust:nightly

RUN apt-get install -y git autoconf automake libtool
RUN git clone https://github.com/sandstorm-io/capnproto \
	&& cd capnproto/c++ && git checkout release-0.5.3 && ./setup-autotools.sh && \
	autoreconf -i && ./configure && make -j6 check && make install
RUN mkdir /code
WORKDIR /code
RUN git clone https://github.com/paenko/paenkodb
WORKDIR /code/paenkodb
RUN cargo build
RUN cd src/document && cargo build

# Raft-rs #

master: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=master)](https://travis-ci.org/paenko/PaenkoDb)
dev: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=dev)](https://travis-ci.org/paenko/PaenkoDb)

# Getting started

First, you need the `nightly` rust compiler. 

```bash
curl -L https://static.rust-lang.org/rustup.sh > rustup
chmod +x rustup
./rustup --channel=nightly
```

Next, you need capnproto

```bash
git clone https://github.com/sandstorm-io/capnproto.git
cd capnproto/c++
./setup-autotools.sh
autoreconf -i
./configure
make -j6 check
sudo make install
```

Finally, clone the repository and build it:

```bash
git clone -b dev https://github.com/paenko/paenkodb && \
cd paenkodb/src/document && \
cargo build && cd ../../
```

Now, you can start a database cluster with docker:

```bash
cd docker && docker-compose up
```
or use the bash:

```bash
./src/document/target/debug/document server <PATH-TO-CONFIG>
```
You can copy the config from `docker_build/config/config.toml`

You can find the test scripts in `test` directory. 

## FAQ ##

> I'm getting the error "failed to run custom build command for `openssl-sys v0.9.8`". How do I fix this?

Follow the instructions on https://github.com/sfackler/rust-openssl - install pkg-config and libssl-dev :+1:

> I'm getting the error "error: could not exec the linker `cc`: No such file or directory (os error 2)" How do I fix this?

You need to install `gcc`.

# Documentation 

* [Raft Crate Documentation](https://hoverbear.github.io/raft-rs/raft/)
* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)

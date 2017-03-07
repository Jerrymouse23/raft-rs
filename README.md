# Raft-rs #

master: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=master)](https://travis-ci.org/paenko/PaenkoDb)
dev: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=dev)](https://travis-ci.org/paenko/PaenkoDb)

# Getting started

First, you need the `nigthly` rust compiler. 

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
git clone -b dev git@github.com:Hoverbear/raft-rs.git && \
cd raft-rs/src/document && \
cargo build && ../../
```

Now, you can start a database cluster with docker:

```bash
docker-compose up
```
or use the bash:

```bash
./src/document/target/debug/document server <PATH-TO-CONFIG>
```
You can copy the config from `docker_build/config/config.toml`

You can find the test scripts in `docker_build/test`. 

## Documentation ##

* [Raft Crate Documentation](https://hoverbear.github.io/raft-rs/raft/)
* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)

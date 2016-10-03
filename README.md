# Raft-rs #

master: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=master)](https://travis-ci.org/paenko/PaenkoDb)
dev: [![Build Status](https://travis-ci.org/paenko/PaenkoDb.svg?branch=dev)](https://travis-ci.org/paenko/PaenkoDb)

## Documentation ##

* [Raft Crate Documentation](https://hoverbear.github.io/raft-rs/raft/)
* [The Raft site](https://raftconsensus.github.io/)
* [The Secret Lives of Data - Raft](http://thesecretlivesofdata.com/raft/)
* [Raft Paper](http://ramcloud.stanford.edu/raft.pdf)
* [Raft Dissertation](https://github.com/ongardie/dissertation#readme)
* [Raft Refloated](https://www.cl.cam.ac.uk/~ms705/pub/papers/2015-osr-raft.pdf)

## Compiling ##

> For Linux, BSD, or Mac. Windows is not supported at this time. We are willing and interested in including support, however none of our contributors work on Windows. Your PRs are welcome!

You will need the [Rust](http://rust-lang.org/) compiler:

```bash
curl -L https://static.rust-lang.org/rustup.sh > rustup
chmod +x rustup
./rustup --channel=nightly
```

> We require the `nightly` channel for now.

This should install `cargo` and `rustc`. Next, you'll need `capnp` to build the
`messages.canpnp` file . It is suggested to use the [git method](https://capnproto.org/install.html#installation-unix)

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
git clone git@github.com:Hoverbear/raft-rs.git && \
cd raft-rs && \
cargo build
```

> Note this is a library, so building won't necessarily produce anything useful for you unless you're developing.

**Document**

```bash
RUST_LOG=raft=debug cargo run --example document server 3000 1 1 127.0.0.1:9000 2 127.0.0.1:9001

RUST_LOG=raft=debug cargo run --example document server 3001  2 1 127.0.0.1:9000 2 127.0.0.1:9001
```

or start with a file

```bash
cd target/debug/examples
RUST_LOG=raft=debug ./document server --config config.toml
```

> config.toml

```toml
  [server]
  node_id = 1
  node_address = "127.0.0.1:9000"
  rest_port = 3000

  [[peers]]
  node_id = 2
  node_address="127.0.0.1:9001"
`

## Testing ##

```bash
RUST_BACKTRACE=1 RUST_LOG=raft=debug cargo test -- --nocapture
```

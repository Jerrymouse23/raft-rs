use auth::hasher::Hasher;

pub struct PlainHasher;

impl Hasher for PlainHasher {
    fn hash(input: &str) -> String {
        input.to_string()
    }
}

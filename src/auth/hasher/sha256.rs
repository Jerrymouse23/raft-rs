use crypto::digest::Digest;
use crypto::sha2::Sha256;

use auth::hasher::Hasher;

pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    fn hash(plain: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.input_str(plain);
        hasher.result_str()
    }
}

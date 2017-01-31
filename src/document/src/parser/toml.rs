use toml::{Parser as omlParser, decode, DecodeError};
use parser::Parser as tParser;
use config::Config;

pub struct Parser;

impl tParser for Parser {
    type Error = DecodeError;

    fn parse(input: &str) -> Result<Config, Self::Error> {
        let toml =
            omlParser::new(&input).parse().expect("An error occurred while parsing the config");

        let decoded: Config = decode(input.parse().unwrap()).unwrap();

        Ok(decoded)
    }
}

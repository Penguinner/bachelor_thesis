use std::error::Error;

pub struct QLeverConnection {
    
    
}

impl QLeverConnection {
    pub fn run_test_query(&mut self, query: &str, rows: usize, columns: usize) -> Result<u128, Box<dyn Error>> {
        // TODO
        Err("Result doesn't match expected size".into())
    }
    
    pub fn new() -> QLeverConnection {
        QLeverConnection {
        }
    }
}
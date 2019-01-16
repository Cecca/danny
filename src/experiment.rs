use chrono::prelude::*;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct Experiment {
    date: String,
    tags: HashMap<String, Value>,
}

impl Experiment {
    pub fn new() -> Experiment {
        let date = Utc::now().to_rfc3339();
        let tags = HashMap::new();
        Experiment { date, tags }
    }

    pub fn tag<T>(mut self, name: &str, value: T) -> Self
    where
        T: Into<Value>,
    {
        self.tags.insert(name.to_owned(), value.into());
        self
    }
}

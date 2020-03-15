use std::collections::HashMap;
use crate::message::{Query, Action, QueryResult};

pub const QUERY_RESULT_SUCCESS: u8 = 0x00;
pub const QUERY_RESULT_REDIRECT: u8 = 0x01;
pub const QUERY_RESULT_ERROR: u8 = 0x11;
pub const QUERY_RESULT_CANDIDATE: u8 = 0x12;
pub const QUERY_RESULT_RETRY: u8 = 0x13;

#[derive(Debug)]
pub struct StateMachine {
    data: HashMap<String, String>,
}

impl StateMachine {

    pub fn new() -> StateMachine {
        StateMachine {
            data: HashMap::new(),
        }
    }

    pub fn execute_query(&mut self, query: Query) -> QueryResult {
        match query.action {
            Action::Save => {
                match query.value {
                    None =>
                        QueryResult {
                            error: QUERY_RESULT_ERROR,
                            message: "value is mandatory".to_string(),
                            value: None
                        },
                    Some(value) => {
                        match self.data.insert(query.key, value) {
                            Some(old_value) =>
                                QueryResult {
                                    error: QUERY_RESULT_SUCCESS,
                                    message: "key existed, old value returned".to_string(),
                                    value: Some(old_value)
                                },
                            None =>
                                QueryResult{
                                    error: QUERY_RESULT_SUCCESS,
                                    message: "new key created".to_string(),
                                    value: None
                                },
                        }
                    }
                }
            }
            Action::Get => {
                match self.data.get(&query.key) {
                    Some(value) =>
                        QueryResult {
                            error: QUERY_RESULT_SUCCESS,
                            message: "key found".to_string(),
                            value: Some(value.clone())
                        },
                    None =>
                        QueryResult{
                            error: QUERY_RESULT_SUCCESS,
                            message: "key not found".to_string(),
                            value: None
                        },
                }
            }
            Action::Delete => {
                match self.data.remove(&query.key) {
                    Some(old_value) =>
                        QueryResult {
                            error: QUERY_RESULT_SUCCESS,
                            message: "key removed, old value returned".to_string(),
                            value: Some(old_value)
                        },
                    None =>
                        QueryResult{
                            error: QUERY_RESULT_SUCCESS,
                            message: "key not found".to_string(),
                            value: None
                        },
                }
            }
        }
    }
}
use serde::{Serialize, Deserialize};

pub const QUERY_TYPE_GET: u8 = 0x01;
pub const QUERY_TYPE_POST: u8 = 0x02;
pub const QUERY_TYPE_PUT: u8 = 0x04;
pub const QUERY_TYPE_DELETE: u8 = 0x08;

pub const QUERY_RESULT_SUCCESS: u8 = 0x00;
pub const QUERY_RESULT_REDIRECT: u8 = 0x01;
pub const QUERY_RESULT_RETRY: u8 = 0x12;

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryRaw {
    action: u8,
    key: String,
    value: String,
}
impl QueryRaw {
    pub fn new(action: u8, key: String, value: String) -> QueryRaw {
        QueryRaw{
            action,
            key,
            value
        }
    }
    fn to_query(self) -> Option<Query> {
        Query::new(self)
    }
}
enum Action {
    Get,
    Post,
    Put,
    Delete,
}
pub struct Query {
    action: Action,
    key: String,
    value: String,
}

impl Query {
    pub fn new(raw_query: QueryRaw) -> Option<Query> {
        let action = match raw_query.action {
            QUERY_TYPE_GET => Some(Action::Get),
            QUERY_TYPE_POST => Some(Action::Post),
            QUERY_TYPE_PUT => Some(Action::Put),
            QUERY_TYPE_DELETE => Some(Action::Delete),
            _ => None,
        };
        if let Some(a) = action {
            Some(Query{
                action: a,
                key: raw_query.key,
                value: raw_query.value,
            })
        }
        else {
            None
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Result {
    error: u8,
    message: String,
    value: String,
}
impl Result {
    pub fn new(error: u8, message: String, value: String) -> Result {
        Result{
            error,
            message,
            value
        }
    }
}
use std::collections::HashMap;
#[cfg(feature = "mocks")]
use mockall::automock;

use crate::structs::*;
use crate::errors::*;

#[cfg_attr(feature = "mocks", automock)]
pub trait DatabaseClient {
    fn save(self: &mut Self) -> Result<(), DatabaseError>;
    fn create_table(self: &mut Self, table: Table) -> Result<(), DatabaseError>;
    fn list_tables(self: &mut Self) -> Result<Vec<String>, DatabaseError>;
    fn drop_table(self: &mut Self, table: &String) -> Result<(), DatabaseError>;
    fn insert(self: &mut Self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn insert_or_update(self: &mut Self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn update(self: &mut Self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn get(self: &mut Self, table: String, primary_field: Field) -> Result<Entry, DatabaseError>;
    fn delete(self: &mut Self, table: String, primary_field: Field) -> Result<(), DatabaseError>;
    fn delete_many(self: &mut Self, table: String, criteria: HashMap<String, Field>) -> Result<u64, DatabaseError>;
    fn scan(self: &mut Self, table: String) -> Result<Vec<Entry>, DatabaseError>;
    fn query(self: &mut Self, table: String, criteria: HashMap<String, Field>) -> Result<Vec<Entry>, DatabaseError>;
    fn prune(self: &mut Self) -> Result<(), DatabaseError>;
}
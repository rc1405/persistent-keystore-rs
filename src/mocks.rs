use std::path::Path;
use std::time::Duration;
use std::collections::HashMap;
use crate::errors::*;
use crate::structs::*;

pub trait PersistentClient {
    fn new<P: AsRef<Path> + Clone + std::fmt::Debug>(path: P, sync_interval: Option<Duration>) -> Result<Box<Self>, DatabaseError>;
    fn open<P: AsRef<Path> + Clone + std::fmt::Debug>(path: P) -> Result<Box<Self>, DatabaseError>;
    fn save(&mut self) -> Result<(), DatabaseError>;
    fn create_table(&mut self, table: Table) -> Result<(), DatabaseError>;
    fn list_tables(&mut self) -> Result<Vec<String>, DatabaseError>;
    fn drop_table(&mut self, table: &String) -> Result<(), DatabaseError>;
    fn insert(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn insert_or_update(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn update(&mut self, table: String, entry: Entry) -> Result<(), DatabaseError>;
    fn get(&mut self, table: String, primary_field: Field) -> Result<Entry, DatabaseError>;
    fn delete(&mut self, table: String, primary_field: Field) -> Result<(), DatabaseError>;
    fn delete_many(&mut self, table: String, criteria: HashMap<String, Field>) -> Result<u64, DatabaseError>;
    fn scan(&mut self, table: String) -> Result<Vec<Entry>, DatabaseError>;
    fn query(&mut self, table: String, criteria: HashMap<String, Field>) -> Result<Vec<Entry>, DatabaseError>;
    fn prune(&mut self) -> Result<(), DatabaseError>;
}

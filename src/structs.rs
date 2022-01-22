use std::time::{SystemTime, Duration};
use std::collections::HashMap;
use std::hash::Hash;
use serde_derive::{Serialize, Deserialize};
use std::fmt;

use crate::errors::*;

#[derive(Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Field {
    String(String),
    I64(i64),
    I32(i32),
    U64(u64),
    U32(u32),
    Date(SystemTime),
    NotImplemented,
}

impl Field {
    pub fn get_type(&self) -> FieldType {
        let t = match self {
            Field::String(_) => FieldType::String,
            Field::I32(_) => FieldType::I32,
            Field::I64(_) => FieldType::I64,
            Field::U64(_) => FieldType::U64,
            Field::U32(_) => FieldType::U32,
            Field::Date(_) => FieldType::Date,
            Field::NotImplemented => FieldType::None,
        };
        t
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match &self {
            Field::String(v) => format!("{}", v),
            Field::I32(v) => format!("{}", v),
            Field::I64(v) => format!("{}", v),
            Field::U64(v) => format!("{}", v),
            Field::U32(v) => format!("{}", v),
            Field::Date(v) => format!("{:?}", v),
            Field::NotImplemented => format!("NotImplemented"),
        };
        write!(f, "{}", msg)
    }
}

#[derive(Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum FieldType {
    String,
    I64,
    I32,
    U64,
    U32,
    Date,
    None,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum FieldRequirement {
    Required(FieldType),
    Optional(FieldType)
}

impl FieldRequirement {
    pub fn unwrap(&self) -> FieldType {
        match self {
            FieldRequirement::Required(f) => return f.clone(),
            FieldRequirement::Optional(f) => return f.clone(),
        }
    }
}

/// Database; a collection of Tables
#[derive(Serialize, Deserialize, Clone)]
pub struct Database {
    pub sync_interval: Option<Duration>,
    tables: HashMap<String, Table>,
}

impl Default for Database {
    fn default() -> Self {
        Self{
            sync_interval: None,
            tables: HashMap::new(),
        }
    }
}

impl Database {
    /// Sets the Sync Duration of the Database.
    /// 
    /// Note this is currently only utilized by the Client
    /// ```
    /// use persistent_keystore_rs::Database;
    /// use std::time::Duration;
    /// 
    /// let mut database = Database::default();
    /// database.set_sync_duration(Duration::from_secs(60));
    /// ```
    pub fn set_sync_duration(&mut self, duration: Duration) {
        self.sync_interval = Some(duration)
    }

    /// Removes the Sync Duration of the Database.
    /// 
    /// Note this is currently only utilized by the Client
    /// ```
    /// use persistent_keystore_rs::Database;
    /// 
    /// let mut database = Database::default();
    /// database.remove_sync_duration();
    /// ```
    pub fn remove_sync_duration(&mut self) {
        self.sync_interval = None
    }

    /// Returns a mutable reference to a Table within the Database
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::Database;
    /// # use std::time::Duration;
    /// 
    /// # let table1 = Table::new()
    /// #    .name("MyTable".to_string())
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field("Count".to_string(), FieldType::I64).unwrap()
    /// #    .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(60))
    /// #    .build().unwrap();
    /// let mut database = Database::default();
    /// # database.create_table(table1).unwrap();
    /// let table = database.get_table(&"MyTable".to_string()).unwrap();
    /// ```
    pub fn get_table(&mut self, table: &String) -> Result<&mut Table, DatabaseError> {
        match self.tables.get_mut(table) {
            Some(t) => return Ok(t),
            None => return Err(DatabaseError::TableDoesNotExist(table.clone()))
        };
    }

    /// Creates a Table within the Database
    /// ```
    /// use persistent_keystore_rs::{Database, Table, FieldType};
    /// use std::time::Duration;
    /// 
    /// let table = Table::new()
    ///     .name("MyTable".to_string())
    ///     .primary_field(FieldType::String).unwrap()
    ///     .add_field("Count".to_string(), FieldType::I64).unwrap()
    ///     .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    ///     .add_expiration(Duration::from_secs(60))
    ///     .build().unwrap();
    /// 
    /// let mut database = Database::default();
    /// database.create_table(table).unwrap();
    /// ```
    pub fn create_table(&mut self, table: Table) -> Result<(), DatabaseError> {
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    /// Deletes the specified Table within the Database
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::Database;
    /// # use std::time::Duration;
    /// 
    /// # let table1 = Table::new()
    /// #    .name("MyTable".to_string())
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field("Count".to_string(), FieldType::I64).unwrap()
    /// #    .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(60))
    /// #    .build().unwrap();
    /// let mut database = Database::default();
    /// # database.create_table(table1).unwrap();
    /// database.drop_table(&"MyTable".to_string()).unwrap();
    /// ```
    pub fn drop_table(&mut self, table: &String) -> Result<(), DatabaseError> {
        match self.tables.remove(table) {
            Some(_) => return Ok(()),
            None => return Err(DatabaseError::TableDoesNotExist(table.clone())),
        }
    }

    /// Returns a Vec of Table names that are stored within the Database
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::Database;
    /// # use std::time::Duration;
    /// 
    /// let mut database = Database::default();
    /// # let table1 = Table::new()
    /// #    .name("MyTable".to_string())
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field("Count".to_string(), FieldType::I64).unwrap()
    /// #    .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(60))
    /// #    .build().unwrap();
    /// # database.create_table(table1).unwrap();
    /// # let table2 = Table::new()
    /// #    .name("MySecondTable".to_string())
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field("Count".to_string(), FieldType::I64).unwrap()
    /// #    .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    /// #    .add_expiration(Duration::from_secs(60))
    /// #    .build().unwrap();
    /// # database.create_table(table2).unwrap();
    /// let tables = database.list_tables();
    /// # assert_eq!(tables.len(), 2);
    /// # assert!(tables.contains(&"MyTable".to_string()));
    /// # assert!(tables.contains(&"MySecondTable".to_string()));
    /// ```
    pub fn list_tables(&mut self) -> Vec<String> {
        let mut results = Vec::new();
        for k in self.tables.keys() {
            results.push(k.clone());
        };
        results
    }
}

/// Builder Pattern for creating a new Table
pub struct TableBuilder {
    table: Table,
}

impl TableBuilder {
    /// Sets the Name of the Table
    /// ```
    /// use persistent_keystore_rs::Table;
    /// let table = Table::new()
    ///     .name("MyTable".to_string());
    /// ```
    pub fn name(mut self, name: String) -> Self {
        self.table.name = name;
        self
    }

    /// Sets the FieldType of the primary_field
    /// ```
    /// use persistent_keystore_rs::{Table, FieldType};
    /// let table = Table::new()
    /// #     .name("MyTable".to_string())
    ///     .primary_field(FieldType::String).unwrap();
    /// ```
    pub fn primary_field(mut self, priary_key: FieldType) -> Result<Self, DatabaseError> {
        if let FieldType::None = priary_key {
            return Err(DatabaseError::UnsupportedFieldType)
        };
        self.table.primary_field = priary_key;
        Ok(self)
    }

    /// Add a required field of FieldType and name to the Table
    /// ```
    /// use persistent_keystore_rs::{Table, FieldType};
    /// let table = Table::new()
    /// #     .name("MyTable".to_string())
    /// #     .primary_field(FieldType::String).unwrap()
    ///     .add_field("Count".to_string(), FieldType::I64).unwrap();
    /// ```
    pub fn add_field(mut self, key: String, field_type: FieldType) -> Result<Self, DatabaseError> {
        if let FieldType::None = field_type {
            return Err(DatabaseError::UnsupportedFieldType)
        };

        self.table.fields.insert(key, FieldRequirement::Required(field_type));
        Ok(self)
    }

    /// Add an optional field of FieldType and name to the Table
    /// ```
    /// use persistent_keystore_rs::{Table, FieldType};
    /// let table = Table::new()
    /// #     .name("MyTable".to_string())
    /// #     .primary_field(FieldType::String).unwrap()
    /// #     .add_field("Count".to_string(), FieldType::I64).unwrap()
    ///     .add_optional_field("Notes".to_string(), FieldType::String).unwrap();
    /// ```
    pub fn add_optional_field(mut self, key: String, field_type: FieldType) -> Result<Self, DatabaseError> {
        if let FieldType::None = field_type {
            return Err(DatabaseError::UnsupportedFieldType)
        };

        self.table.fields.insert(key, FieldRequirement::Optional(field_type));
        Ok(self)
    }

    /// Add an expiration Duration to the Table.  The timer for the expiration
    /// is set to the last time an Entry was updated.  
    /// 
    /// Note that this does not happen automatically within this struct; but is utilized
    /// within Client
    /// ```
    /// use persistent_keystore_rs::{Table, FieldType};
    /// use std::time::Duration;
    /// 
    /// let table = Table::new()
    /// #     .name("MyTable".to_string())
    /// #     .primary_field(FieldType::String).unwrap()
    /// #     .add_field("Count".to_string(), FieldType::I64).unwrap()
    /// #     .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    ///     .add_expiration(Duration::from_secs(60));
    /// ```
    pub fn add_expiration(mut self, expire_after: Duration) -> Self {
        self.table.expire_after = Some(expire_after);
        self
    }

    /// Validates the Table is properly configured and returns the Table object.
    /// ```
    /// use persistent_keystore_rs::{Table, FieldType};
    /// use std::time::Duration;
    /// 
    /// let table = Table::new()
    ///     .name("MyTable".to_string())
    ///     .primary_field(FieldType::String).unwrap()
    ///     .add_field("Count".to_string(), FieldType::I64).unwrap()
    ///     .add_optional_field("Notes".to_string(), FieldType::String).unwrap()
    ///     .add_expiration(Duration::from_secs(60))
    ///     .build();
    /// ```
    pub fn build(self) -> Result<Table, DatabaseError> {
        if let FieldType::None = self.table.primary_field {
            return Err(DatabaseError::TableMissingPrimaryKey)

        } else if self.table.name.len() == 0 {
            return Err(DatabaseError::TableNameNotSet)

        } else if self.table.fields.len() == 0 {
            return Err(DatabaseError::TableMustContainFields)
        };

        Ok(self.table)
    }
}

/// Table is a collection of Entry objects that meet a specified format criteria
#[derive(Serialize, Deserialize, Clone)]
pub struct Table {
    pub name: String,
    pub primary_field: FieldType,
    pub fields: HashMap<String, FieldRequirement>,
    entries: HashMap<Field, Entry>,
    pub expire_after: Option<Duration>,
}

impl Table {
    /// Returns a TableBuilder Instance that will be used to create a new table
    /// ```
    /// use persistent_keystore_rs::Table;
    /// let table = Table::new();
    /// ```
    pub fn new() -> TableBuilder {
        TableBuilder{
            table: Table{
                name: String::new(),
                primary_field: FieldType::None,
                fields: HashMap::new(),
                entries: HashMap::new(),
                expire_after: None,
            }
        }
    }

    /// Returns a reference to an Entry within the Table matching the primary Field
    /// If the primary Field does not exist, DatabaseError::EntryDoesNotExists is returned.
    /// ```
    /// # use persistent_keystore_rs::{Table, Entry, FieldType};
    /// use persistent_keystore_rs::Field;
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert(entry).unwrap();
    /// let result = table.get(&Field::String("MyFirstEntry".to_string())).unwrap();
    /// ```
    pub fn get(&self, key: &Field) -> Result<&Entry, DatabaseError> {
        match self.entries.get_key_value(key) {
            Some((_, v)) => return Ok(v),
            None => return Err(DatabaseError::EntryDoesNotExists),
        }
    }

    /// Inserts the provided entry into the Table
    /// If the primary Field exists, DatabaseError::EntryExists is returned.
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// let entry = Entry::new()
    ///    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///    .build().unwrap();
    /// table.insert(entry).unwrap();
    /// ```
    pub fn insert(&mut self, mut entry: Entry) -> Result<(), DatabaseError> {
        self.validate_field_types(&entry)?;
        self.validate_required_fields(&entry)?;
        entry.last_timestamp = Some(SystemTime::now());

        match self.get(&entry.primary_field) {
            Ok(_) => return Err(DatabaseError::EntryExists),
            Err(_) => {
                
                match self.entries.insert(entry.primary_field.clone(), entry) {
                    Some(_) => {},
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// Inserts the provided entry if it doesn't exist, or updates if it does
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// let entry = Entry::new()
    ///    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///    .build().unwrap();
    /// 
    /// table.insert_or_update(entry).unwrap();
    /// 
    /// let entry2 = Entry::new()
    ///    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///    .build().unwrap();
    /// 
    /// table.insert_or_update(entry2).unwrap();
    /// ```
    pub fn insert_or_update(&mut self, entry: Entry) -> Result<(), DatabaseError> {
        return self.update(entry)
    }

    /// Updates the provided entry within the Table.
    /// If the Entry does not exist, DatabaseError:EntryDoesNotExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Table, FieldType};
    /// use persistent_keystore_rs::{Entry, Field};
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # let entry2 = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert_or_update(entry2).unwrap();
    /// 
    /// let entry = Entry::new()
    ///    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///    .build().unwrap();
    /// 
    /// table.update(entry).unwrap();
    /// ```
    pub fn update(&mut self, mut entry: Entry) -> Result<(), DatabaseError> {
        self.validate_field_types(&entry)?;
        self.validate_required_fields(&entry)?;
        entry.last_timestamp = Some(SystemTime::now());

        match self.entries.insert(entry.primary_field.clone(), entry) {
            Some(_) => {},
            None => {}
        }
        Ok(())
    }

    /// Validates that all required fields are provided and that no fields are provided
    /// that are not configured in the table.
    fn validate_required_fields(&self, entry: &Entry) -> Result<(), DatabaseError> {
        let mut fields: Vec<&String> = Vec::new();
        for k in self.fields.keys() {
            fields.push(k);
        };

        for (k, _) in &entry.fields {
            if self.fields.contains_key(k) {
                match fields.iter().position(|r| r == &k) {
                    Some(index) => {
                        fields.remove(index);
                    },
                    None => return Err(DatabaseError::UnsupportedField(k.clone())),
                }
            } else {
                return Err(DatabaseError::UnsupportedField(k.clone()))
            }
        }

        for f in fields {
            match self.fields.get_key_value(f) {
                Some((_, requirement)) => {
                    match requirement {
                        FieldRequirement::Required(_) => return Err(DatabaseError::MissingRequiredField(f.clone())),
                        FieldRequirement::Optional(_) => {},
                    }
                },
                None => {}
            }
        };

        Ok(())
    }

    /// Validates that the fields provided within the entry, matches the field types
    /// of the Entry with the field types specified in the table.
    fn validate_field_types(&self, entry: &Entry) -> Result<(), DatabaseError> {
        if self.primary_field != entry.primary_field.get_type() {
            return Err(DatabaseError::MismatchedFieldType)
        };

        for (k, v) in &entry.fields {
            match self.fields.get_key_value(k) {
                Some((_, value)) => {
                    if value.clone().unwrap() != v.get_type() {
                        return Err(DatabaseError::MismatchedFieldType)
                    }
                },
                None => {},
            }
        }
        Ok(())
    }

    /// Deletes the entry with the associated primary_field from the Table.
    /// If the Entry does not exist, DatabaseError:EntryDoesNotExists is returned
    /// ```
    /// # use persistent_keystore_rs::{Table, Entry, FieldType};
    /// use persistent_keystore_rs::Field;
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert_or_update(entry).unwrap();
    /// table.delete(Field::String("MyFirstEntry".to_string())).unwrap();
    /// ```
    pub fn delete(&mut self, primary_field: Field) -> Result<(), DatabaseError> {
        match self.entries.remove_entry(&primary_field) {
            Some(_) => return Ok(()),
            None => return Err(DatabaseError::EntryDoesNotExists),
        }
    }

    /// Returns all Entries from the Table
    /// ```
    /// # use persistent_keystore_rs::{Table, Entry, Field, FieldType};
    /// # use std::time::{Duration, SystemTime};
    /// # let mut table = Table::new()
    /// #    .name(String::from("MyTable"))
    /// #    .primary_field(FieldType::String).unwrap()
    /// #    .add_field(String::from("TimeStamp"), FieldType::Date).unwrap()
    /// #    .add_expiration(Duration::from_secs(2592000))
    /// #    .build().unwrap();
    /// # let entry = Entry::new()
    /// #    .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert(entry).unwrap();
    /// # let entry2 = Entry::new()
    /// #    .set_primary_field(Field::String("MySecondEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert(entry2).unwrap();
    /// # let entry3 = Entry::new()
    /// #    .set_primary_field(Field::String("MyThirdEntry".to_string())).unwrap()
    /// #    .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    /// #    .build().unwrap();
    /// # table.insert(entry3).unwrap();
    /// let results = table.scan().unwrap();
    /// # assert_eq!(results.len(), 3);
    /// ```
    pub fn scan(&self) -> Result<Vec<Entry>, DatabaseError> {
        let mut results = Vec::new();
        for (_, v) in &self.entries {
            results.push(v.clone())
        };
        Ok(results)
    }
}

/// Entry represents all items that are contained within a Table
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub primary_field: Field,
    pub fields: HashMap<String, Field>,
    pub last_timestamp: Option<SystemTime>,
}

impl Entry {
    /// Returns an EntryBuilder Instance that will be used to create a new entry
    /// ```
    /// use persistent_keystore_rs::Entry;
    /// let entry = Entry::new();
    /// ```
    pub fn new() -> EntryBuilder {
        EntryBuilder{
            entry: Entry{
                primary_field: Field::NotImplemented,
                fields: HashMap::new(),
                last_timestamp: None,
            }
        }
    }

    /// Returns an Optional Field value for a given Entry
    /// ```
    /// use persistent_keystore_rs::{Entry, Field};
    /// use std::time::SystemTime;
    /// let entry = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("TimeStamp".to_string(), Field::Date(SystemTime::now())).unwrap()
    ///     .build().unwrap();
    /// 
    /// let exists = &entry.get_field("TimeStamp".to_string());
    /// # if let None = exists {
    /// #   panic!("Field should exist");
    /// # };
    /// 
    /// # if let Some(_) = &entry.get_field("DoesNotExist".to_string()) {
    /// #   panic!("Field should not exist");
    /// # }
    /// ```
    /// 
    pub fn get_field(&self, key: String) -> Option<Field> {
        if let Some((_, v)) = self.fields.get_key_value(&key) {
            return Some(v.clone());
        };
        None
    }
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match &self.primary_field {
            Field::String(v) => format!("{}", v),
            Field::I32(v) => format!("{}", v),
            Field::I64(v) => format!("{}", v),
            Field::U64(v) => format!("{}", v),
            Field::U32(v) => format!("{}", v),
            Field::Date(v) => format!("{:?}", v),
            Field::NotImplemented => format!("NotImplemented"),
        };
        write!(f, "{}", msg)
    }
}

/// Builder Pattern for creating new Entry items to be inserted into a Table
pub struct EntryBuilder {
    entry: Entry,
}

impl EntryBuilder {
    /// Sets the primary_field of the Entry
    /// ```
    /// use persistent_keystore_rs::{Entry, Field};
    /// let mut entry_builder = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap();
    /// ```
    pub fn set_primary_field(mut self, field: Field) -> Result<Self, DatabaseError> {
        if let FieldType::None = field.get_type() {
            return Err(DatabaseError::InvalidPrimaryKey)
        };
        self.entry.primary_field = field;
        Ok(self)
    }

    /// Add a field to the Entry.  This can include both required and optional fields.
    /// ```
    /// use persistent_keystore_rs::{Entry, Field};
    /// let mut entry_builder = Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("Count".to_string(), Field::I32(0)).unwrap();
    /// ```
    pub fn add_field(mut self, key: String, field: Field) -> Result<Self, DatabaseError> {
        if let FieldType::None = field.get_type() {
            return Err(DatabaseError::UnsupportedFieldType)
        };

        if key.len() == 0 {
            return Err(DatabaseError::InvalidPrimaryKey)
        }

        self.entry.fields.insert(key, field);
        Ok(self)
    }

    /// Validates the Entry is properly formatted with a primary field and contains at least one
    /// value field.
    /// ```
    /// use persistent_keystore_rs::{Entry, Field};
    /// let entry =  Entry::new()
    ///     .set_primary_field(Field::String("MyFirstEntry".to_string())).unwrap()
    ///     .add_field("Count".to_string(), Field::I32(3)).unwrap()
    ///     .build().unwrap();
    /// ```
    pub fn build(self) -> Result<Entry, DatabaseError> {
        if let FieldType::None = self.entry.primary_field.get_type() {
            return Err(DatabaseError::InvalidPrimaryKey)
        };

        if self.entry.fields.len() == 0 {
            return Err(DatabaseError::EntryMustContainFields)
        }

        Ok(self.entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_field_some() {
        let entry = Entry::new()
            .set_primary_field(Field::String("This should Succeed too".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(12312312)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second entry".to_string())).unwrap()
            .build().unwrap();

        if let None = entry.get_field("FirstKey".to_string()) {
            panic!("Expected value, received none")
        }
    }

    #[test]
    fn entry_field_none() {
        let entry = Entry::new()
            .set_primary_field(Field::String("This should Succeed too".to_string())).unwrap()
            .add_field("FirstKey".to_string(), Field::I64(12312312)).unwrap()
            .add_field("OptionalKey".to_string(), Field::String("My second entry".to_string())).unwrap()
            .build().unwrap();

        if let Some(s) = entry.get_field("MissingKey".to_string()) {
            panic!("Expected None, received {}", s)
        }
    }
}
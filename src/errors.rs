
use std::fmt;
use crate::structs::Database;
use std::sync::PoisonError;

#[derive(Debug)]
pub enum DatabaseError {
    TableExists(String),
    TableDoesNotExist(String),
    TableMissingPrimaryKey,
    TableNameNotSet,
    TableMustContainFields,
    EntryMustContainFields,
    EntryExists,
    EntryDoesNotExists,
    DatabaseIoError(std::io::Error),
    DatabaseExistsError,
    DatabaseDoesNotExist(String),
    UnsupportedField(String),
    MissingRequiredField(String),
    MismatchedFieldType,
    UnsupportedFieldType,
    DatabaseSerializationError(Box<bincode::ErrorKind>),
    UnableToGetLock,
    InvalidPrimaryKey,
    DatabaseDecompressionError(lz4_flex::block::DecompressError),
    DatabaseCompressionError(lz4_flex::block::CompressError),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            DatabaseError::TableMustContainFields => format!("Table must contain at least one field"),
            DatabaseError::TableNameNotSet => format!("Table name must not be empty"),
            DatabaseError::TableMissingPrimaryKey => format!("Missing primary key from table"),
            DatabaseError::TableExists(t) => format!("Table {} already exists", t),
            DatabaseError::TableDoesNotExist(t) => format!("Table {} does not exist", t),
            DatabaseError::EntryExists => format!("Entry exists"),
            DatabaseError::EntryDoesNotExists => format!("Entry does not exist"),
            DatabaseError::DatabaseExistsError => format!("Database already exists"),
            DatabaseError::DatabaseIoError(e) => format!("Database IO Error: {}", e),
            DatabaseError::DatabaseDoesNotExist(d) => format!("Database {} is not found", d),
            DatabaseError::UnsupportedField(f) => format!("Field name {} is not supported on table", f),
            DatabaseError::MissingRequiredField(f) => format!("Missing required field {}", f),
            DatabaseError::MismatchedFieldType => format!("Field is not a supported type"),
            DatabaseError::UnsupportedFieldType => format!("Field is not a supported type"),
            DatabaseError::DatabaseSerializationError(e) => format!("Received Database Serialization error: {}", e),
            DatabaseError::UnableToGetLock => format!("Unable to acquire lock on database"),
            DatabaseError::InvalidPrimaryKey => format!("Invalid primary key"),
            DatabaseError::EntryMustContainFields => format!("Entry must contain at least one field"),
            DatabaseError::DatabaseCompressionError(e) => format!("Database compression error {}", e),
            DatabaseError::DatabaseDecompressionError(e) => format!("Database decompression error {}", e),
        };
        write!(f, "{}", msg)
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(e: std::io::Error) -> DatabaseError {
        DatabaseError::DatabaseIoError(e)
    }
}

impl From<Box<bincode::ErrorKind>> for DatabaseError {
    fn from(e: Box<bincode::ErrorKind>) -> DatabaseError {
        DatabaseError::DatabaseSerializationError(e)
    }
}

impl From<PoisonError<Database>> for DatabaseError {
    fn from(_: PoisonError<Database>) -> DatabaseError {
        DatabaseError::UnableToGetLock
    }
}

impl From<PoisonError<&mut Database>> for DatabaseError {
    fn from(_: PoisonError<&mut Database>) -> DatabaseError {
        DatabaseError::UnableToGetLock
    }
}

impl From<lz4_flex::block::DecompressError> for DatabaseError {
    fn from(e: lz4_flex::block::DecompressError) -> DatabaseError {
        DatabaseError::DatabaseDecompressionError(e)
    }
}

impl From<lz4_flex::block::CompressError> for DatabaseError {
    fn from(e: lz4_flex::block::CompressError) -> DatabaseError {
        DatabaseError::DatabaseCompressionError(e)
    }
}
#[cfg(all(test, feature = "mocks"))]
use persistent_keystore_rs::MockDatabaseClient;
#[cfg(all(test, feature = "mocks"))]
use mockall::predicate;
#[allow(unused_imports)]
use persistent_keystore_rs::prelude::*;
#[allow(unused_imports)]
use persistent_keystore_rs::{Field, Entry};
#[allow(unused_imports)]
use persistent_keystore_rs::errors::DatabaseError;
#[allow(unused_imports)]
use std::collections::HashMap;

#[test]
fn test_mock_importable() {
    #[cfg(all(test, feature = "mocks"))]
    {
        let _mockdb = MockDatabaseClient::new();
    }
}

#[test]
fn test_mock_no_table_exists() {
    #[cfg(all(test, feature = "mocks"))]
    {
        let mut mockdb = MockDatabaseClient::new();
        mockdb
            .expect_get()
            .with(predicate::eq("MyTable".to_string()), predicate::eq(Field::String("MyField".to_string())))
            .returning(|x, _y| return Err(DatabaseError::TableDoesNotExist(x.to_string()))
        );

        let results = mockdb.get("MyTable".to_string(), Field::String("MyField".to_string()));

        match results {
            Ok(_) => panic!("Error expected"),
            Err(e) => assert_eq!(format!("{}", e), "Table MyTable does not exist".to_string()),
        };

    }
}

#[test]
fn test_mock_get_item() {
    #[cfg(all(test, feature = "mocks"))]
    {
        let mut mockdb = MockDatabaseClient::new();
        mockdb
            .expect_get()
            .with(predicate::eq("MyTable".to_string()), predicate::eq(Field::String("MyField".to_string())))
            .returning(|_x, y| return Ok(Entry{
                primary_field: y,
                fields: HashMap::new(),
                last_timestamp: None,
            })
        );

        let results = mockdb.get("MyTable".to_string(), Field::String("MyField".to_string()));

        match results {
            Ok(r) => {
                assert_eq!(Entry{
                    primary_field: Field::String("MyField".to_string()),
                    fields: HashMap::new(),
                    last_timestamp: None,
                }, r)
            },
            Err(e) => panic!("No error expected, received {}", e),
        };

    }
}
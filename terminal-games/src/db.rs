// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use std::{future::Future, path::Path};

use bb8::{ManageConnection, Pool};

pub type DbPool = Pool<LibsqlConnectionManager>;

pub struct LibsqlConnectionManager {
    database: libsql::Database,
}

impl LibsqlConnectionManager {
    pub async fn new_local_pool(db_path: impl AsRef<Path>) -> libsql::Result<DbPool> {
        Self::build_pool(Self::new_local(db_path).await?).await
    }

    pub async fn new_remote_pool(
        url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> libsql::Result<DbPool> {
        Self::build_pool(Self::new_remote(url, auth_token).await?).await
    }

    async fn build_pool(manager: Self) -> Result<DbPool, libsql::Error> {
        Pool::builder()
            .max_size(4)
            .test_on_check_out(true)
            .build(manager)
            .await
    }

    async fn new_local(db_path: impl AsRef<Path>) -> libsql::Result<Self> {
        let database = libsql::Builder::new_local(db_path.as_ref()).build().await?;
        Ok(Self { database })
    }

    async fn new_remote(
        url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> libsql::Result<Self> {
        let database = libsql::Builder::new_remote(url.into(), auth_token.into())
            .build()
            .await?;
        Ok(Self { database })
    }
}

impl ManageConnection for LibsqlConnectionManager {
    type Connection = libsql::Connection;
    type Error = libsql::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        async move { self.database.connect() }
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            conn.query("SELECT 1", ()).await?;
            Ok(())
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

package mysql

import "github.com/temporalio/temporal/schema/mysql"

// NOTE: whenever there is a new data base schema update, plz update the following versions

// Version is the Postgres database release version
// Temporal supports both MySQL and Postgres officially, so upgrade should be perform for both MySQL and Postgres
const Version = mysql.Version

// VisibilityVersion is the Postgres visibility database release version
// Temporal supports both MySQL and Postgres officially, so upgrade should be perform for both MySQL and Postgres
const VisibilityVersion = mysql.VisibilityVersion

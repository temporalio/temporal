package schema

import "log"

// SetupTask represents a task
// that sets up cassandra schema on
// a specified keyspace
type SetupTask struct {
	db     DB
	config *SetupConfig
}

func newSetupSchemaTask(db DB, config *SetupConfig) *SetupTask {
	return &SetupTask{
		db:     db,
		config: config,
	}
}

// Run executes the task
func (task *SetupTask) Run() error {
	config := task.config
	log.Printf("Starting schema setup, config=%+v\n", config)

	if config.Overwrite {
		err := task.db.DropAllTables()
		if err != nil {
			return err
		}
	}

	if !config.DisableVersioning {
		log.Printf("Setting up version tables\n")
		if err := task.db.CreateSchemaVersionTables(); err != nil {
			return err
		}
	}

	if len(config.SchemaFilePath) > 0 {
		stmts, err := ParseFile(config.SchemaFilePath)
		if err != nil {
			return err
		}

		log.Println("----- Creating types and tables -----")
		for _, stmt := range stmts {
			log.Println(rmspaceRegex.ReplaceAllString(stmt, " "))
			if err := task.db.Exec(stmt); err != nil {
				return err
			}
		}
		log.Println("----- Done -----")
	}

	if !config.DisableVersioning {
		log.Printf("Setting initial schema version to %v\n", config.InitialVersion)
		err := task.db.UpdateSchemaVersion(config.InitialVersion, config.InitialVersion)
		if err != nil {
			return err
		}
		log.Printf("Updating schema update log\n")
		err = task.db.WriteSchemaUpdateLog("0", config.InitialVersion, "", "initial version")
		if err != nil {
			return err
		}
	}

	log.Println("Schema setup complete")

	return nil
}

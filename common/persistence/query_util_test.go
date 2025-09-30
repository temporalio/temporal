package persistence

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
)

type (
	queryUtilSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		logger log.Logger
	}
)

func TestQueryUtilSuite(t *testing.T) {
	s := new(queryUtilSuite)
	suite.Run(t, s)
}

func (s *queryUtilSuite) SetupTest() {
	s.logger = log.NewTestLogger()
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *queryUtilSuite) TestLoadAndSplitQueryFromReaders() {
	input := `
		DO LANGUAGE 'plpgsql' $$
			BEGIN
				IF ( NOT EXISTS (select extname from pg_extension where extname = 'btree_gin') ) THEN
					CREATE EXTENSION btree_gin;
				END  IF; --Intentionally add multiple spaces between END and IF
			END
		$$;

		CREATE TABLE test (
			id BIGINT not null,
			col1 BIGINT, -- comment with unmatched parenthesis )
			col2 VARCHAR(255),
			PRIMARY KEY (id)
		);

		CREATE INDEX test_idx ON test (col1);

		--begin
		CREATE TRIGGER test_ai AFTER INSERT ON test
		BEGIN
			SELECT *, 'string with unmatched chars ")' FROM test;
			--end
		END;

		-- trailing comment
	`
	statements, err := LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.NoError(err)
	s.Equal(4, len(statements))
	s.Equal(
		`DO LANGUAGE 'plpgsql' $$
			BEGIN
				IF ( NOT EXISTS (select extname from pg_extension where extname = 'btree_gin') ) THEN
					CREATE EXTENSION btree_gin;
				END  IF;
			END
		$$;`,
		statements[0],
	)
	s.Equal(
		`CREATE TABLE test (
			id BIGINT not null,
			col1 BIGINT,
			col2 VARCHAR(255),
			PRIMARY KEY (id)
		);`,
		statements[1],
	)
	s.Equal(`CREATE INDEX test_idx ON test (col1);`, statements[2])
	// comments are removed, but the inner content is not trimmed
	s.Equal(
		`CREATE TRIGGER test_ai AFTER INSERT ON test
		BEGIN
			SELECT *, 'string with unmatched chars ")' FROM test;
			
		END;`,
		statements[3],
	)

	input = "CREATE TABLE test (;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched left parenthesis")
	s.Nil(statements)

	input = "CREATE TABLE test ());"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched right parenthesis")
	s.Nil(statements)

	input = "begin"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched `BEGIN` keyword")
	s.Nil(statements)

	input = "end"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched `END` keyword")
	s.Nil(statements)

	input = "select ' from test;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	s.Error(err, "error reading contents: unmatched quotes")
	s.Nil(statements)
}

func (s *queryUtilSuite) TestHasWordAt() {
	s.True(hasWordAt("BEGIN", "BEGIN", 0))
	s.True(hasWordAt(" BEGIN ", "BEGIN", 1))
	s.True(hasWordAt(")BEGIN;", "BEGIN", 1))
	s.False(hasWordAt("BEGIN", "BEGIN", 1))
	s.False(hasWordAt("sBEGIN", "BEGIN", 1))
	s.False(hasWordAt("BEGINs", "BEGIN", 0))
	s.False(hasWordAt("7BEGIN", "BEGIN", 1))
	s.False(hasWordAt("BEGIN7", "BEGIN", 0))
}

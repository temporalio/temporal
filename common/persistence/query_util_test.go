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
		// not merely log an error
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
	require.NoError(s.T(), err)
	require.Equal(s.T(), 4, len(statements))
	require.Equal(s.T(),
		`DO LANGUAGE 'plpgsql' $$
			BEGIN
				IF ( NOT EXISTS (select extname from pg_extension where extname = 'btree_gin') ) THEN
					CREATE EXTENSION btree_gin;
				END  IF;
			END
		$$;`,
		statements[0],
	)
	require.Equal(s.T(),
		`CREATE TABLE test (
			id BIGINT not null,
			col1 BIGINT,
			col2 VARCHAR(255),
			PRIMARY KEY (id)
		);`,
		statements[1],
	)
	require.Equal(s.T(), `CREATE INDEX test_idx ON test (col1);`, statements[2])
	// comments are removed, but the inner content is not trimmed
	require.Equal(s.T(),
		`CREATE TRIGGER test_ai AFTER INSERT ON test
		BEGIN
			SELECT *, 'string with unmatched chars ")' FROM test;
			
		END;`,
		statements[3],
	)

	input = "CREATE TABLE test (;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	require.Error(s.T(), err, "error reading contents: unmatched left parenthesis")
	require.Nil(s.T(), statements)

	input = "CREATE TABLE test ());"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	require.Error(s.T(), err, "error reading contents: unmatched right parenthesis")
	require.Nil(s.T(), statements)

	input = "begin"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	require.Error(s.T(), err, "error reading contents: unmatched `BEGIN` keyword")
	require.Nil(s.T(), statements)

	input = "end"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	require.Error(s.T(), err, "error reading contents: unmatched `END` keyword")
	require.Nil(s.T(), statements)

	input = "select ' from test;"
	statements, err = LoadAndSplitQueryFromReaders([]io.Reader{bytes.NewBufferString(input)})
	require.Error(s.T(), err, "error reading contents: unmatched quotes")
	require.Nil(s.T(), statements)
}

func (s *queryUtilSuite) TestHasWordAt() {
	require.True(s.T(), hasWordAt("BEGIN", "BEGIN", 0))
	require.True(s.T(), hasWordAt(" BEGIN ", "BEGIN", 1))
	require.True(s.T(), hasWordAt(")BEGIN;", "BEGIN", 1))
	require.False(s.T(), hasWordAt("BEGIN", "BEGIN", 1))
	require.False(s.T(), hasWordAt("sBEGIN", "BEGIN", 1))
	require.False(s.T(), hasWordAt("BEGINs", "BEGIN", 0))
	require.False(s.T(), hasWordAt("7BEGIN", "BEGIN", 1))
	require.False(s.T(), hasWordAt("BEGIN7", "BEGIN", 0))
}

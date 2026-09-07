package tdbg_test

import "go.temporal.io/server/tools/tdbg"

func (s *utilSuite) TestStringToEnum_MapCaseInsensitive() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := tdbg.StringToEnum("timeR", enumValues)
	s.NoError(err)
	s.Equal(int32(2), result) // Timer
}

func (s *utilSuite) TestStringToEnum_MapNonExisting() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := tdbg.StringToEnum("Timer2", enumValues)
	s.Error(err)
	s.Equal(int32(0), result)
}

func (s *utilSuite) TestStringToEnum_MapEmptyValue() {
	enumValues := map[string]int32{
		"Unspecified": 0,
		"Transfer":    1,
		"Timer":       2,
		"Replication": 3,
	}

	result, err := tdbg.StringToEnum("", enumValues)
	s.NoError(err)
	s.Equal(int32(0), result)
}

func (s *utilSuite) TestStringToEnum_MapEmptyEnum() {
	enumValues := map[string]int32{}

	result, err := tdbg.StringToEnum("Timer", enumValues)
	s.Error(err)
	s.Equal(int32(0), result)
}

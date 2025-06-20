package authorization

import (
	"testing"
)

func TestInvalidRoles(t *testing.T) {
	testInvalid(t, 32)
	testInvalid(t, 33)
	testInvalid(t, 64)
	testInvalid(t, 125)
}

func TestValidRoles(t *testing.T) {
	testValid(t, RoleUndefined)
	testValid(t, RoleWorker)
	testValid(t, RoleReader)
	testValid(t, RoleWriter)
	testValid(t, RoleAdmin)
	testValid(t, RoleWorker|RoleReader)
	testValid(t, RoleWorker|RoleWriter)
	testValid(t, RoleWorker|RoleAdmin)
	testValid(t, RoleWorker|RoleReader|RoleWriter)
	testValid(t, RoleWorker|RoleReader|RoleAdmin)
	testValid(t, RoleWorker|RoleReader|RoleWriter|RoleAdmin)
	testValid(t, RoleReader|RoleWriter)
	testValid(t, RoleReader|RoleAdmin)
	testValid(t, RoleReader|RoleWriter|RoleAdmin)
}

func testValid(t *testing.T, value Role) {
	if !value.IsValid() {
		t.Errorf("Valid role value %d reported as invalid.", value)
	}
}
func testInvalid(t *testing.T, value Role) {
	if value.IsValid() {
		t.Errorf("Invalid role value %d reported as valid.", value)
	}
}

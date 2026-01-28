package failing

import "testing"

func TestAlwaysFails(t *testing.T) { t.Fatal("always fails") }
func TestOK(t *testing.T)          { t.Log("ok") }

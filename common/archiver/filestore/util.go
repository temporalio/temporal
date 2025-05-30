package filestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	historypb "go.temporal.io/api/history/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/codec"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

var (
	errDirectoryExpected  = errors.New("a path to a directory was expected")
	errFileExpected       = errors.New("a path to a file was expected")
	errEmptyDirectoryPath = errors.New("directory path is empty")
)

// File I/O util

func fileExists(filepath string) (bool, error) {
	if info, err := os.Stat(filepath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if info.IsDir() {
		return false, errFileExpected
	}
	return true, nil
}

func directoryExists(path string) (bool, error) {
	if info, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	} else if !info.IsDir() {
		return false, errDirectoryExpected
	}
	return true, nil
}

func mkdirAll(path string, dirMode os.FileMode) error {
	return os.MkdirAll(path, dirMode)
}

func writeFile(filepath string, data []byte, fileMode os.FileMode) (retErr error) {
	if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.Create(filepath)
	defer func() {
		err := f.Close()
		if err != nil {
			retErr = err
		}
	}()
	if err != nil {
		return err
	}
	if err = f.Chmod(fileMode); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	return nil
}

// readFile reads the contents of a file specified by filepath
// WARNING: callers of this method should be extremely careful not to use it in a context where filepath is supplied by
// the user.
func readFile(filepath string) ([]byte, error) {
	// #nosec
	return os.ReadFile(filepath)
}

func listFiles(dirPath string) (fileNames []string, err error) {
	if info, err := os.Stat(dirPath); err != nil {
		return nil, err
	} else if !info.IsDir() {
		return nil, errDirectoryExpected
	}

	f, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Combine(err, f.Close())
	}()
	return f.Readdirnames(-1)
}

func listFilesByPrefix(dirPath string, prefix string) ([]string, error) {
	fileNames, err := listFiles(dirPath)
	if err != nil {
		return nil, err
	}

	var filteredFileNames []string
	for _, name := range fileNames {
		if strings.HasPrefix(name, prefix) {
			filteredFileNames = append(filteredFileNames, name)
		}
	}
	return filteredFileNames, nil
}

// encoding & decoding util

func encode(message proto.Message) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.Encode(message)
}

func encodeHistories(histories []*historypb.History) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.EncodeHistories(histories)
}

func decodeVisibilityRecord(data []byte) (*archiverspb.VisibilityRecord, error) {
	record := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err := encoder.Decode(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func deserializeQueryVisibilityToken(bytes []byte) (*queryVisibilityToken, error) {
	token := &queryVisibilityToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

// File name construction

func constructHistoryFilename(namespaceID, workflowID, runID string, version int64) string {
	combinedHash := constructHistoryFilenamePrefix(namespaceID, workflowID, runID)
	return fmt.Sprintf("%s_%v.history", combinedHash, version)
}

func constructHistoryFilenamePrefix(namespaceID, workflowID, runID string) string {
	return strings.Join([]string{hash(namespaceID), hash(workflowID), hash(runID)}, "")
}

func constructVisibilityFilename(closeTimestamp time.Time, runID string) string {
	return fmt.Sprintf("%v_%s.visibility", closeTimestamp.UnixNano(), hash(runID))
}

func hash(s string) string {
	return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
}

// Validation

func validateDirPath(dirPath string) error {
	if len(dirPath) == 0 {
		return errEmptyDirectoryPath
	}
	info, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errDirectoryExpected
	}
	return nil
}

// Misc.

func extractCloseFailoverVersion(filename string) (int64, error) {
	filenameParts := strings.FieldsFunc(filename, func(r rune) bool {
		return r == '_' || r == '.'
	})
	if len(filenameParts) != 3 {
		return -1, errors.New("unknown filename structure")
	}
	return strconv.ParseInt(filenameParts[1], 10, 64)
}

func historyMutated(request *archiver.ArchiveHistoryRequest, historyBatches []*historypb.History, isLast bool) bool {
	lastBatch := historyBatches[len(historyBatches)-1].Events
	lastEvent := lastBatch[len(lastBatch)-1]
	lastFailoverVersion := lastEvent.GetVersion()
	if lastFailoverVersion > request.CloseFailoverVersion {
		return true
	}

	if !isLast {
		return false
	}
	lastEventID := lastEvent.GetEventId()
	return lastFailoverVersion != request.CloseFailoverVersion || lastEventID+1 != request.NextEventID
}

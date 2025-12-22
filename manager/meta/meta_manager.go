package meta

import (
	"bufio"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
)

type MetaManager struct {
	schemas map[string]*schema.Schema
	lock    sync.RWMutex

	storagePath string
}

func (sm *MetaManager) getAbsStoragePath(segments ...string) string {

	pathSegments := []string{sm.storagePath}
	pathSegments = append(pathSegments, segments...)

	return filepath.Join(pathSegments...)
}

func NewMetaManager(storagePath string) *MetaManager {
	return &MetaManager{
		schemas: map[string]*schema.Schema{},
		lock:    sync.RWMutex{},

		storagePath: storagePath,
	}
}

func (qp *MetaManager) AddSchema(schemaObject *schema.Schema) {

	qp.lock.Lock()
	defer qp.lock.Unlock()

	qp.schemas[schemaObject.Name] = schemaObject
}

func (qp *MetaManager) GetSchema(name string) *schema.Schema {
	qp.lock.RLock()
	defer qp.lock.RUnlock()

	return qp.schemas[name]
}

func (m *MetaManager) StoreSchemeToDisk(schemeObject schema.Schema) error {
	schemesPath := m.getAbsStoragePath(schemeObject.Name, "schema.json")

	fr := io.NewFileReader(schemesPath)
	createFileErr := fr.Open(false)

	if createFileErr != nil {
		return createFileErr
	}

	defer fr.Close()

	jschemeBytes, _ := json.Marshal(schemeObject)

	linesWriter := bufio.NewWriter(fr.Raw())
	linesWriter.Write(jschemeBytes)
	return linesWriter.Flush()

}
func (m *MetaManager) LoadSchemesFromDisk() error {

	entries, err := os.ReadDir(m.storagePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // no schemes yet
			return nil
		} else {
			log.Printf(" >>>>>>> %v", err)
			return err
		}
	}

	loadSingleSchemeFileFromDisk := func(path string) error {

		schemaFilePathName := m.getAbsStoragePath(path, "schema.json")

		fullContent, contentErr := os.ReadFile(schemaFilePathName)
		if contentErr != nil {
			return contentErr
		}

		var schema schema.Schema
		err = json.Unmarshal(fullContent, &schema)
		if err != nil {
			return err
		} else {
			m.AddSchema(&schema)
			slog.Info(" loaded schema from disk", "schema_name", schema.Name)

			// for _, column := range schema.Columns {
			// 	for _, colSlab := range column.Slabs {

			// 		uidTime := colSlab.Time()
			// 		seconds, ns := uidTime.UnixTime()

			// 		oTime := time.Unix(seconds, ns)

			// 		slog.Info("slab for column loaded", "column_name", column.Name, "time", oTime.String())
			// 	}
			// }

		}

		return nil
	}

	for _, e := range entries {
		if e.IsDir() {
			loadSingleSchemeFileFromDisk(e.Name())
		}
	}

	return nil
}

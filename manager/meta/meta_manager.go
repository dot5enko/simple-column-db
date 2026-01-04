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
	"time"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type MetaManager struct {
	schemas map[string]*schema.Schema
	lock    sync.RWMutex

	storagePath string

	rtLock        sync.RWMutex
	schemaRtCache map[string]*SchemaRtCache
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

		storagePath:   storagePath,
		schemaRtCache: make(map[string]*SchemaRtCache),
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

type ColumnPrecachedInfo struct {
	BlocksPerSlab int16
}

type SchemaRtCache struct {
	Columns        map[string]ColumnPrecachedInfo
	SlabsByColumns map[string][]uuid.UUID

	MaxBlocks int
	Created   time.Time
}

func (m *MetaManager) getRtCache(name string) *SchemaRtCache {
	m.rtLock.RLock()
	defer m.rtLock.RUnlock()

	return m.schemaRtCache[name]
}

func (m *MetaManager) GetCacheForSchema(s *schema.Schema) (*SchemaRtCache, error) {

	if rtCache := m.getRtCache(s.Name); rtCache != nil {
		return rtCache, nil
	}

	newEntry := &SchemaRtCache{
		Columns:        map[string]ColumnPrecachedInfo{},
		SlabsByColumns: map[string][]uuid.UUID{},
	}

	// collect slabs
	maxBlocks := 0
	for _, it := range s.Columns {

		fieldBlocksPerSlab := it.Type.BlocksPerSlab()
		newEntry.Columns[it.Name] = ColumnPrecachedInfo{
			BlocksPerSlab: fieldBlocksPerSlab,
		}

		if len(it.Slabs) > 0 {

			// global
			// slabsFiltered = append(slabsFiltered, it.Slabs...)

			old, isOk := newEntry.SlabsByColumns[it.Name]
			if !isOk {
				old = []uuid.UUID{}
				newEntry.SlabsByColumns[it.Name] = old
			}

			newEntry.SlabsByColumns[it.Name] = append(old, it.Slabs...)

			slabsSize := len(newEntry.SlabsByColumns[it.Name])
			blocksAtMax := slabsSize * int(fieldBlocksPerSlab)
			if blocksAtMax > maxBlocks {
				maxBlocks = blocksAtMax
			}
		}
	}

	newEntry.MaxBlocks = maxBlocks

	m.rtLock.Lock()
	m.schemaRtCache[s.Name] = newEntry
	m.rtLock.Unlock()

	return newEntry, nil
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

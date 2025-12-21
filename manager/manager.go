package manager

import (
	"bufio"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

type BlockRuntimeInfo struct {
	Val          *schema.RuntimeBlockData
	Synchronized bool
	Header       schema.DiskHeader
}

type ManagerConfig struct {
	PathToStorage string

	CacheMaxBytes uint64
}

type Manager struct {
	schemas map[string]*schema.Schema

	config ManagerConfig

	Slabs SlabManager

	BlockBuffer [schema.TotalHeaderSize]byte
}

func (m *Manager) storeSchemeToDisk(schemeObject schema.Schema) error {
	schemesPath := m.Slabs.getAbsStoragePath(schemeObject.Name, "schema.json")

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
func (m *Manager) loadSchemesFromDisk() error {

	entries, err := os.ReadDir(m.config.PathToStorage)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) { // no schemes yet
			return nil
		} else {
			log.Printf(" >>>>>>> %v", err)
			return err
		}
	}

	loadSingleSchemeFileFromDisk := func(path string) error {

		schemaFilePathName := m.Slabs.getAbsStoragePath(path, "schema.json")

		fullContent, contentErr := os.ReadFile(schemaFilePathName)
		if contentErr != nil {
			return contentErr
		}

		var schema schema.Schema
		err = json.Unmarshal(fullContent, &schema)
		if err != nil {
			return err
		} else {
			m.schemas[schema.Name] = &schema
			slog.Info(" loaded schema from disk", "schema_name", schema.Name)

			for _, column := range schema.Columns {
				for _, colSlab := range column.Slabs {

					uidTime := colSlab.Time()
					seconds, ns := uidTime.UnixTime()

					oTime := time.Unix(seconds, ns)

					slog.Info("slab for column loaded", "column_name", column.Name, "time", oTime.String())
				}
			}

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

func New(config ManagerConfig) *Manager {

	man := &Manager{
		schemas: make(map[string]*schema.Schema),
		config:  config,
		Slabs: SlabManager{
			storagePath: config.PathToStorage,
			// caches
			cache:         map[[32]byte]BlockCacheItem{},
			slabCacheItem: map[uuid.UUID]*SlabCacheItem{},
		},
	}

	loadErr := man.loadSchemesFromDisk()
	if loadErr != nil {
		panic(loadErr) // todo return error
	}

	return man

}

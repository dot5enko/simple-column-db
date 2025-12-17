package manager

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/fatih/color"
	"github.com/google/uuid"
)

type BlockRuntimeInfo struct {
	Val          any
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
	schemesPath := m.getAbsStoragePath("schemes.json")

	fr := io.NewFileReader(schemesPath)
	createFileErr := fr.Open(false)
	if createFileErr != nil { // file not exists - create new one
		return createFileErr
	}

	defer fr.Close()

	jschemeBytes, _ := json.Marshal(schemeObject)

	linesWriter := bufio.NewWriter(fr.Raw())
	linesWriter.Write(jschemeBytes)
	linesWriter.WriteByte('\n')
	return linesWriter.Flush()

}
func (m *Manager) loadSchemesFromDisk() error {

	schemesPath := m.getAbsStoragePath("schemes.json")

	f, err := os.Open(schemesPath)

	if err == nil {

		defer f.Close()

		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

		for {

			if !sc.Scan() {
				if err := sc.Err(); err != nil {
					return err
				}
				return nil
			}

			var schema schema.Schema
			err = json.Unmarshal(sc.Bytes(), &schema)
			if err != nil {
				return err
			} else {
				m.schemas[schema.Name] = &schema
				color.Yellow(" >> loaded schema from disk : %s", schema)
			}
		}
	}

	return nil
}

func New(config ManagerConfig) *Manager {

	man := &Manager{
		schemas: make(map[string]*schema.Schema),
		config:  config,
		Slabs: SlabManager{
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

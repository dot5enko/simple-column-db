package manager

import (
	"os"
	"path/filepath"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
)

func (sm *Manager) getAbsStoragePath(segments ...string) string {

	pathSegments := []string{sm.config.PathToStorage}
	pathSegments = append(pathSegments, segments...)

	return filepath.Join(pathSegments...)
}

func (sm *Manager) createStoragePathIfNotExists(segments ...string) (string, error) {
	storagePath := sm.getAbsStoragePath(segments...)

	if _, err := os.Stat(storagePath); err != nil {
		storageFolderErr := os.MkdirAll(storagePath, 0644)
		if storageFolderErr != nil {
			return "", storageFolderErr
		}
	}

	return storagePath, nil
}

func (sm *Manager) CreateSchema(schemaConfig schema.Schema) error {

	path, err := sm.createStoragePathIfNotExists("storage", schemaConfig.Name)

	if err != nil {
		return err
	}

	headerBuffer := make([]byte, schema.TotalHeaderSize*(schema.SlabBlocks+3))

	// for each column create slab on disk
	for _, col := range schemaConfig.Columns {

		slabHeader, slabError := schema.NewDiskSlab(schemaConfig, col.Name)
		if slabError != nil {
			return slabError
		}

		slabPath := sm.getAbsStoragePath(path, col.Name, slabHeader.Uid.String()+".slab")

		writtenBytes, writeErr := slabHeader.WriteTo(headerBuffer)
		if writeErr != nil {
			return writeErr
		}

		fileManager := io.NewFileReader(slabPath)
		fileWriteErr := fileManager.WriteAt(headerBuffer[:writtenBytes], 0, writtenBytes)
		if fileWriteErr != nil {
			return fileWriteErr
		}
	}

	return nil
}

package manager

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (sm *Manager) getAbsStoragePath(segments ...string) string {

	pathSegments := []string{sm.config.PathToStorage}
	pathSegments = append(pathSegments, segments...)

	return filepath.Join(pathSegments...)
}

func (sm *Manager) createStoragePathIfNotExists(segments ...string) (string, error) {
	storagePath := sm.getAbsStoragePath(segments...)

	if _, err := os.Stat(storagePath); err != nil {
		storageFolderErr := os.MkdirAll(storagePath, 0755)
		if storageFolderErr != nil {

			log.Printf("unable to create directory : %s", storagePath)

			return "", storageFolderErr
		} else {
			log.Printf(" >> created %s folder", storagePath)
		}
	}

	return storagePath, nil
}

func (sm *Manager) GetSlabPath(s schema.Schema, id uuid.UUID) string {
	return sm.getAbsStoragePath(s.Name, id.String()+".slab")
}

func (sm *Manager) CreateSchema(schemaConfig schema.Schema) error {

	_, err := sm.createStoragePathIfNotExists(schemaConfig.Name)

	if err != nil {
		return fmt.Errorf("unable to create schema folder: `%s`", err.Error())
	}

	headerBuffer := make([]byte, schema.TotalHeaderSize*3)

	// for each column create slab on disk
	for _, col := range schemaConfig.Columns {

		createOneSlabForColumn := func() error {
			slabHeader, slabError := schema.NewDiskSlab(schemaConfig, col.Name)
			if slabError != nil {
				return slabError
			}

			slabPath := sm.GetSlabPath(schemaConfig, slabHeader.Uid)

			writtenBytes, writeErr := slabHeader.WriteTo(headerBuffer)
			if writeErr != nil {
				return writeErr
			}

			fileManager := io.NewFileReader(slabPath)
			fileManager.OpenForReadOnly(false)

			defer fileManager.Close()

			fileWriteErr := fileManager.WriteAt(headerBuffer[:writtenBytes], 0, writtenBytes)
			if fileWriteErr != nil {
				return fileWriteErr
			}

			// last unfinished block + headers for all blocks inside
			headersReservedSpace := (int(slabHeader.BlocksTotal) + 1) * int(schema.TotalHeaderSize)
			zeroesFilledErr := fileManager.FillZeroes(writtenBytes, headersReservedSpace)

			if zeroesFilledErr != nil {
				return zeroesFilledErr
			}

			// reserve space for block entries
			// calc
			reservedSize := int(slabHeader.SingleBlockRowsSize) * int(slabHeader.BlocksTotal) * slabHeader.Type.Size()
			fillContentErr := fileManager.FillZeroes(writtenBytes+headersReservedSpace, reservedSize)

			return fillContentErr
		}

		slabCreationErr := createOneSlabForColumn()
		if slabCreationErr != nil {
			return slabCreationErr
		}

	}

	return nil
}

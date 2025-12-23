package meta

import (
	"log"
	"os"
	"path/filepath"

	"github.com/dot5enko/simple-column-db/io"
	"github.com/dot5enko/simple-column-db/schema"
	"github.com/google/uuid"
)

func (sm *SlabManager) getAbsStoragePath(segments ...string) string {

	pathSegments := []string{sm.storagePath}
	pathSegments = append(pathSegments, segments...)

	return filepath.Join(pathSegments...)
}

func (sm *SlabManager) createStoragePathIfNotExists(segments ...string) (string, error) {
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

func (sm *SlabManager) GetSlabPath(s schema.Schema, id uuid.UUID) string {
	return sm.getAbsStoragePath(s.Name, id.String()+".slab")
}

func (sm *SlabManager) GetSlabFile(s schema.Schema, id uuid.UUID, writeAccess bool) (*io.FileReader, error) {

	slabPath := sm.GetSlabPath(s, id)

	fileManager := io.NewFileReader(slabPath)
	openErr := fileManager.Open(!writeAccess)

	// log.Printf(" --- opening[write:%v] : %s", writeAccess, slabPath)

	return fileManager, openErr
}

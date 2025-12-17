package manager

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dot5enko/simple-column-db/bits"
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

func (sm *Manager) GetSlabFile(s schema.Schema, id uuid.UUID, writeAccess bool) (*io.FileReader, error) {

	slabPath := sm.GetSlabPath(s, id)

	fileManager := io.NewFileReader(slabPath)
	openErr := fileManager.Open(!writeAccess)

	// log.Printf(" --- opening[write:%v] : %s", writeAccess, slabPath)

	return fileManager, openErr
}

func (sm *Manager) preallocateSlab(s schema.Schema, uid uuid.UUID) error {

	fileManager, err := sm.GetSlabFile(s, uid, true)

	if err != nil {
		return err
	}

	defer fileManager.Close()

	// hard guess that block headers are no more than 20% of slab size
	return fileManager.FillZeroes(0, int(float64(schema.SlabDiskContentsUncompressed)*1.2))
}

func (sm *Manager) UpdateBlockHeaderAndDataOnDisk(
	s schema.Schema,
	slab *schema.DiskSlabHeader,
	block *schema.RuntimeBlockData,
) error {

	foundIdx := -1
	for idx, it := range slab.BlockHeaders {
		if it.Uid == block.Header.Uid {
			foundIdx = idx
			break
		}
	}

	if foundIdx == -1 {
		return fmt.Errorf("block with uid `%s` doesn't exist in slab", block.Header.Uid.String())
	}

	buf := bits.NewEncodeBuffer(sm.BlockBuffer[:], binary.LittleEndian)
	serializedBytes, headerBytesErr := block.Header.WriteTo(&buf)

	if headerBytesErr != nil {
		return fmt.Errorf("unable to serialize block header, header won't serialize : %s", headerBytesErr.Error())
	} else {

		singleBlockUncompressedSize := slab.Type.Size() * schema.BlockRowsSize
		blockDataOffset := singleBlockUncompressedSize * foundIdx

		headersHeaderOffset := schema.TotalHeaderSize * uint64(foundIdx)
		slabHeaderAbsOffset := schema.SlabHeaderFixedSize + headersHeaderOffset
		headersSize := schema.TotalHeaderSize * uint64(slab.BlocksTotal)

		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to get slab file : %s", slabErr.Error())
		}

		defer fileManager.Close()

		headerBlockUpdateErr := fileManager.WriteAt(sm.BlockBuffer[:], int(slabHeaderAbsOffset), serializedBytes)
		if headerBlockUpdateErr != nil {
			return fmt.Errorf("unable to update block header : %s", headerBlockUpdateErr.Error())
		}

		writeBuf := bytes.NewBuffer(sm.Slabs.BufferForCompressedData10Mb[:0])
		writeErr := io.DumpNumbersArrayBlockAny(writeBuf, block.DataTypedArray)
		if writeErr != nil {
			return fmt.Errorf("unable to finalize block : %s", writeErr.Error())
		}

		// update block content

		// writeBuf.Bytes()
		slab := sm.Slabs.GetSlabFromCache(slab.Uid)

		copy(slab.data[blockDataOffset:], writeBuf.Bytes())

		return fileManager.WriteAt(slab.data[:], int(schema.SlabHeaderFixedSize+headersSize), schema.SlabDiskContentsUncompressed)
	}

	// return nil
}

func (sm *Manager) UpdateSlabHeaderOnDisk(s schema.Schema, slab *schema.DiskSlabHeader) error {

	serializedBytes, headerBytesErr := slab.WriteTo(sm.Slabs.SlabHeaderReaderBuffer[:])
	if headerBytesErr != nil {
		return fmt.Errorf("unable to finalize block, slab header won't serialize : %s", headerBytesErr.Error())
	} else {

		fileManager, slabErr := sm.GetSlabFile(s, slab.Uid, true)
		if slabErr != nil {
			return fmt.Errorf("unable to update slab header : %s", slabErr.Error())
		}

		defer fileManager.Close()
		return fileManager.WriteAt(sm.Slabs.SlabHeaderReaderBuffer[:], 0, serializedBytes)
	}
}

func (sm *Manager) CreateSchemaIfNotExists(schemaConfig schema.Schema) error {

	storagePath := sm.getAbsStoragePath(schemaConfig.Name)

	_, err := os.Stat(storagePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("unable to check schema folder existence : %s", err.Error())
		} // path does not exist
	} else {
		return nil
	}

	_, err = sm.createStoragePathIfNotExists(schemaConfig.Name)

	if err != nil {
		return fmt.Errorf("unable to create schema folder: `%s`", err.Error())
	}

	// for each column create slab on disk
	for colIdx, col := range schemaConfig.Columns {
		createOneSlabForColumn := func() error {

			slabHeader, slabError := schema.NewDiskSlab(schemaConfig, col.Name)
			if slabError != nil {
				return slabError
			}

			preallocateErr := sm.preallocateSlab(schemaConfig, slabHeader.Uid)
			if preallocateErr != nil {
				return fmt.Errorf("unable to preallocate slab : %s", preallocateErr.Error())
			}

			slabHeaderWriteErr := sm.UpdateSlabHeaderOnDisk(schemaConfig, slabHeader)
			if slabHeaderWriteErr != nil {
				return slabHeaderWriteErr
			}

			f, slabFileErr := sm.GetSlabFile(schemaConfig, slabHeader.Uid, true)

			if slabFileErr != nil {
				return fmt.Errorf("unable to open slab file : %s", slabFileErr.Error())
			}

			// crete first block
			firstBlock := schema.NewBlockHeader(col.Type)
			headerWriter := bits.NewEncodeBuffer(sm.BlockBuffer[:], binary.LittleEndian)
			writtenBytes, writeErr := firstBlock.WriteTo(&headerWriter)
			if writeErr != nil {
				return fmt.Errorf("unable to encode block header : %s", writeErr.Error())
			}

			writeToDiskErr := f.WriteAt(sm.BlockBuffer[:writtenBytes], schema.SlabHeaderFixedSize, writtenBytes)
			if writeToDiskErr != nil {
				return fmt.Errorf("unable to write block header into slab : %s", writeToDiskErr.Error())
			}

			// headers for blocks inside
			headersReservedSpace := int(slabHeader.BlocksTotal-1) * int(schema.TotalHeaderSize)
			reservedSize := int(slabHeader.SingleBlockRowsSize) * int(slabHeader.BlocksTotal) * slabHeader.Type.Size()

			totalZeroSize := headersReservedSpace + reservedSize

			zeroesFilledErr := f.FillZeroes(schema.SlabHeaderFixedSize+schema.TotalHeaderSize, totalZeroSize)

			if zeroesFilledErr != nil {
				return zeroesFilledErr
			}

			curCol := &schemaConfig.Columns[colIdx]

			curCol.Slabs = []uuid.UUID{slabHeader.Uid}
			curCol.ActiveSlab = slabHeader.Uid
			// load from disk upon start

			return nil
		}

		slabCreationErr := createOneSlabForColumn()
		if slabCreationErr != nil {
			return slabCreationErr
		}

	}

	sm.schemas[schemaConfig.Name] = &schemaConfig

	return nil
}

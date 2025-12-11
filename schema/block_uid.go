package schema

import "github.com/google/uuid"

type BlockUniqueId [17]byte

func (block BlockUniqueId) GetGroupAndId() (uuid.UUID, uint8) {

	guid, err := uuid.FromBytes(block[:16])

	if err != nil {
		return uuid.Nil, 0
	}

	return guid, uint8(block[16])
}

func NewBlockUniqueId(guid uuid.UUID, index uint8) BlockUniqueId {
	var blockid BlockUniqueId
	copy(blockid[:], guid[:])
	blockid[16] = byte(index)

	return blockid
}

func (block BlockUniqueId) MustUid() uuid.UUID {
	gr, _ := block.GetGroupAndId()
	return gr
}

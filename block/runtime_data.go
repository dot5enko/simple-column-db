package block

type RuntimeBlockData[T any] struct {
	Data []T
	Cap  int
}

package BaseServer

type FileOperation struct {
	pathToFileSystemNodePointer map[string]*FileSystemNode
	instanceSeq	uint64		// 用于标记
}

func InitFileOperation() *FileOperation{
	Root := &FileOperation{}

	Root.pathToFileSystemNodePointer = make(map[string]*FileSystemNode)

	return Root
}

var RootFileOperation = InitFileOperation()
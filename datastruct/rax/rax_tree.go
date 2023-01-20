package rax


/*
如果是非压缩节点，有多个字符，对应多个节点，需要对所有字符进行排序
如果是压缩节点，只有一个节点，无需排序
*/
type RaxTreeNode struct {
	isKey bool
	isComp bool
	isNull bool
	size int
	chars []byte
	children []*RaxTreeNode
	data interface{}
}






package types

type SubscriptionHandler interface {
	ProcessCqnData(d []CqnData)
}

type CqnData struct {
	SchemaTableName string
	TableOperation  CqnOpCode
	RowChanges      RowChanges
}

const (
	CqnAllRows CqnOpCode = 1 << iota
	CqnInsert
	CqnUpdate
	CqnDelete
	CqnAlter
	CqnDrop
	CqnUnexpected
)

type RowId string
type CqnOpCode uint32
type RowChanges map[RowId]CqnOpCode

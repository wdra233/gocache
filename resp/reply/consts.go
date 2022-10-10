package reply


type PongReply struct {
}

var pongBytes = []byte("+PONG\r\n")

func (reply *PongReply) ToBytes() []byte {
	return pongBytes
}

var thePongReply = new(PongReply)
func MakePongReply() *PongReply {
	return thePongReply
}

type OkReply struct {
}

var okBytes = []byte("+OK\r\n")
func (reply *OkReply) ToBytes() []byte {
	return okBytes
}

var theOKReply = new(OkReply)
func MakeOkReply() *OkReply {
	return theOKReply
}

type EmptyNullBulkReply struct {
}

var emptyNullBulkReply = []byte("*0\r\n")
func (reply *EmptyNullBulkReply) ToBytes() []byte {
	return emptyNullBulkReply
}

func MakeEmptyNullBulkReply() *EmptyNullBulkReply {
	return &EmptyNullBulkReply{}
}

type NullBulkReply struct {
}

var nullBulkBytes = []byte("$-1\r\n")
func (reply *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

type NoReply struct {
}

var noBytes = []byte("")
func (reply *NoReply) ToBytes() []byte {
	return noBytes
}

func MakeNoReply() *NoReply {
	return &NoReply{}
}

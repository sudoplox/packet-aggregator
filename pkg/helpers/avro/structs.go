package avro

import "os/exec"

type DecodeType int32

const (
	NativeFromTextual = DecodeType(iota)
	BinaryFromNative  = DecodeType(iota)
	NativeFromBinary  = DecodeType(iota)
	TextualFromNative = DecodeType(iota)
)

var (
	SchemaFilePath, _ = exec.Command("git", "rev-parse", "--show-toplevel").Output()
)

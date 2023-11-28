package avro

import (
	"fmt"
	"github.com/linkedin/goavro/v2"
	"os"
	"path/filepath"
	"strings"
)

// TransformAvro
// Output: msg type =>
// 	NativeFromTextual	->	interface | map[string]interface{}
// 	BinaryFromNative 	->	[]byte
// 	NativeFromBinary		->	interface | map[string]interface{}
// 	TextualFromNative	->	[]byte

func TransformAvro(from any, codec *goavro.Codec, transformType DecodeType) (msg interface{}, err error) {
	switch transformType {
	case NativeFromTextual:
		// Convert textual Avro data (in Avro JSON format) to native Go form
		msg, _, err = codec.NativeFromTextual(from.([]byte))

	case BinaryFromNative:
		// Convert native Go form to binary Avro data
		msg, err = codec.BinaryFromNative(nil, from)

	case NativeFromBinary:
		// Convert binary Avro data back to native Go form
		msg, _, err = codec.NativeFromBinary(from.([]byte))

	case TextualFromNative:
		// Convert Go native data types to Avro data in JSON text format
		msg, err = codec.TextualFromNative(nil, from)
	}

	return msg, err
}

func GetCodec(schemaFile string) (*goavro.Codec, error) {

	absPath, err := filepath.Abs(schemaFile)
	if err != nil {
		fmt.Println("Error while getting abs path to schema file: " + err.Error())
	}
	dat, err := os.ReadFile(absPath)
	if err != nil {
		fmt.Println("err while reading avro schema through schemaFile: " + err.Error())
		absPath, _ = filepath.Abs(strings.TrimSuffix(string(SchemaFilePath), "\n") + schemaFile)
		dat, err = os.ReadFile(absPath)
		if err != nil {
			fmt.Println("err while reading avro schema through SchemaFilePath: " + err.Error())
			return nil, err
		}
	}
	codec, err := goavro.NewCodec(string(dat))

	return codec, err
}

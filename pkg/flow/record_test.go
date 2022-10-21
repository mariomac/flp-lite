package flow

import (
	"testing"

	jsonIter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONMarshal(t *testing.T) {
	f := Record{Meta: map[string]interface{}{"a": "b"}}

	b, err := jsonIter.ConfigDefault.Marshal(&f)
	require.NoError(t, err)
	assert.Equal(t,
		`{"FlowDirection":0,"Bytes":0,"SrcAddr":"","DstAddr":"","SrcMac":"","DstMac":"",`+
		`"SrcPort":0,"DstPort":0,"Etype":0,"Packets":0,"Proto":0,"TimeFlowStartMS":0,`+
		`"TimeFlowEndMS":0,"Interface":"","a":"b"}`,
		string(b))
}
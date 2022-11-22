package proto_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jrhy/mast"
	v1 "github.com/jrhy/mast/proto_test/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type LocalKey v1.Key

var layer = mast.DefaultLayer(json.Marshal)
var store = mast.NewInMemoryStore()

func toStringArray(in []interface{}) []string {
	o := make([]string, len(in))
	for i := range in {
		o[i] = in[i].(string)
	}
	return o
}
func marshalProto(i interface{}) ([]byte, error) {
	n := i.(mast.Node)
	o := v1.Node{
		Key:   toStringArray(n.Key),
		Value: toStringArray(n.Value),
		Link:  toStringArray(n.Link),
	}
	return proto.Marshal(&o)
}
func toInterfaceArray(in []string) []interface{} {
	o := make([]interface{}, len(in))
	for i := range in {
		o[i] = in[i]
	}
	return o
}
func unmarshalProto(b []byte, o interface{}) error {
	var in v1.Node
	err := proto.Unmarshal(b, &in)
	if err != nil {
		return fmt.Errorf("unmarshal proto: %w", err)
	}
	*o.(*mast.Node) = mast.Node{
		Key:   toInterfaceArray(in.Key),
		Value: toInterfaceArray(in.Value),
		Link:  toInterfaceArray(in.Link),
	}
	return nil
}

func newTestTree(
	rootOptions *mast.CreateRemoteOptions,
	cfg *mast.RemoteConfig,
) *mast.Mast {
	ctx := context.Background()
	root := mast.NewRoot(rootOptions)
	m, err := root.LoadMast(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return m
}

func (l *LocalKey) Layer(branchFactor uint) uint8 {
	layer, err := layer(l.S, mast.DefaultBranchFactor)
	if err != nil {
		panic(err)
	}
	return layer
}

func (l *LocalKey) Order(o mast.Key) int {
	return strings.Compare(l.S, o.(*LocalKey).S)
}

var _ mast.Key = &LocalKey{}

func mustJSON(i interface{}) string {
	s, err := json.MarshalIndent(i, "", " ")
	if err != nil {
		panic(err)
	}
	return string(s)
}

func TestUseProtoKeysAndValues(t *testing.T) {
	ctx := context.Background()
	k := LocalKey(v1.Key{S: "key"})
	t.Logf("key json:\n%s\n", mustJSON(&k))
	v := v1.Value{S: "value"}
	cfg := mast.RemoteConfig{
		StoreImmutablePartsWith: store,
		KeysLike:                &LocalKey{},
		ValuesLike:              &v1.Value{},
	}
	m := newTestTree(nil, &cfg)
	require.Equal(t, uint64(0), m.Size())
	err := m.Insert(ctx, &k, &v)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.Size())
	kprime := LocalKey(v1.Key{S: "key"})
	err = m.Insert(ctx, &kprime, &v)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.Size())
	var vp *v1.Value
	ok, err := m.Get(ctx, &k, &vp)
	require.True(t, ok)
	require.Equal(t, &v, vp)
	r, err := m.MakeRoot(ctx)
	require.NoError(t, err)
	t.Logf("saved root: %s\n", mustJSON(r))
	m2, err := r.LoadMast(ctx, &cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m2.Size())
	t.Logf("key json:\n%s\n", mustJSON(&k))
	ok, err = m2.Get(ctx, &k, &vp)
	require.NotNil(t, vp)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, proto.Equal(&v1.Value{S: "value"}, vp))
}

func TestProtoMarshaling(t *testing.T) {
	ctx := context.Background()
	cfg := mast.RemoteConfig{
		StoreImmutablePartsWith:        store,
		KeysLike:                       "",
		ValuesLike:                     "",
		Marshal:                        marshalProto,
		Unmarshal:                      unmarshalProto,
		UnmarshalerUsesRegisteredTypes: true,
	}
	m := newTestTree(&mast.CreateRemoteOptions{
		NodeFormat: mast.V1Marshaler,
	}, &cfg)
	require.Equal(t, uint64(0), m.Size())
	err := m.Insert(ctx, "k", "v")
	require.NoError(t, err)
	var vg string
	ok, err := m.Get(ctx, "k", &vg)
	require.True(t, ok)
	require.Equal(t, "v", vg)
	r, err := m.MakeRoot(ctx)
	require.NoError(t, err)
	t.Logf("saved root: %s\n", mustJSON(r))
	m2, err := r.LoadMast(ctx, &cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(1), m2.Size())
	ok, err = m2.Get(ctx, "k", &vg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "v", vg)
}

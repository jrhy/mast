package mast

import (
	"bytes"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/commands"
	"github.com/stretchr/testify/require"
)

func benchmarkStdMapInsert(factor int, b *testing.B) {
	m := map[int]int{}
	for n := 0; n < factor*b.N; n++ {
		m[n] = n
	}
}

func BenchmarkStdMapInsert1(b *testing.B)    { benchmarkStdMapInsert(1, b) }
func BenchmarkStdMapInsert10(b *testing.B)   { benchmarkStdMapInsert(10, b) }
func BenchmarkStdMapInsert100(b *testing.B)  { benchmarkStdMapInsert(100, b) }
func BenchmarkStdMapInsert1k(b *testing.B)   { benchmarkStdMapInsert(1_000, b) }
func BenchmarkStdMapInsert10k(b *testing.B)  { benchmarkStdMapInsert(10_000, b) }
func BenchmarkStdMapInsert100k(b *testing.B) { benchmarkStdMapInsert(100_000, b) }
func BenchmarkStdMapInsert1m(b *testing.B)   { benchmarkStdMapInsert(1_000_000, b) }

func benchmarkStdMapGet(factor int, b *testing.B) {
	m := map[int]int{}
	b.StopTimer()
	for n := 0; n < factor*b.N; n++ {
		m[n] = n
	}
	b.StartTimer()
	for n := 0; n < factor*b.N; n++ {
		_ = m[n]
	}
}

func BenchmarkStdMapGet1(b *testing.B)    { benchmarkStdMapGet(1, b) }
func BenchmarkStdMapGet10(b *testing.B)   { benchmarkStdMapGet(10, b) }
func BenchmarkStdMapGet100(b *testing.B)  { benchmarkStdMapGet(100, b) }
func BenchmarkStdMapGet1k(b *testing.B)   { benchmarkStdMapGet(1_000, b) }
func BenchmarkStdMapGet10k(b *testing.B)  { benchmarkStdMapGet(10_000, b) }
func BenchmarkStdMapGet100k(b *testing.B) { benchmarkStdMapGet(100_000, b) }
func BenchmarkStdMapGet1m(b *testing.B)   { benchmarkStdMapGet(1_000_000, b) }

func benchmarkInsert(factor int, b *testing.B) {
	m := newTestTree(0, "")
	for n := 0; n < factor*b.N; n++ {
		m.Insert(n, n)
	}
}

func BenchmarkInsert1(b *testing.B)    { benchmarkInsert(1, b) }
func BenchmarkInsert10(b *testing.B)   { benchmarkInsert(10, b) }
func BenchmarkInsert100(b *testing.B)  { benchmarkInsert(100, b) }
func BenchmarkInsert1k(b *testing.B)   { benchmarkInsert(1_000, b) }
func BenchmarkInsert10k(b *testing.B)  { benchmarkInsert(10_000, b) }
func BenchmarkInsert100k(b *testing.B) { benchmarkInsert(100_000, b) }
func BenchmarkInsert1m(b *testing.B)   { benchmarkInsert(1_000_000, b) }

func benchmarkGet(factor int, b *testing.B) {
	m := newTestTree(0, "")
	b.StopTimer()
	for n := 0; n < factor*b.N; n++ {
		m.Insert(n, n)
	}
	b.StartTimer()
	var v int
	for n := 0; n < factor*b.N; n++ {
		m.Get(n, &v)
	}
}

func BenchmarkGet1(b *testing.B)    { benchmarkGet(1, b) }
func BenchmarkGet10(b *testing.B)   { benchmarkGet(10, b) }
func BenchmarkGet100(b *testing.B)  { benchmarkGet(100, b) }
func BenchmarkGet1k(b *testing.B)   { benchmarkGet(1_000, b) }
func BenchmarkGet10k(b *testing.B)  { benchmarkGet(10_000, b) }
func BenchmarkGet100k(b *testing.B) { benchmarkGet(100_000, b) }
func BenchmarkGet1m(b *testing.B)   { benchmarkGet(1_000_000, b) }

func BenchmarkExerciser(b *testing.B) {
	parameters := gopter.DefaultTestParametersWithSeed(1593228262585360000)
	parameters.MaxSize = 2048
	parameters.MinSuccessfulTests = b.N
	properties := gopter.NewProperties(parameters)
	properties.Property("mast exerciser", commands.Prop(mastCommands))
	out := bytes.NewBuffer(nil)
	reporter := gopter.NewFormatedReporter(false, 98, out)
	require.True(b, properties.Run(reporter))
}

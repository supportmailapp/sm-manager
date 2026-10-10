package supervisor

import (
	"reflect"
	"testing"
)

func TestBatchClusterIDs(t *testing.T) {
	cases := []struct {
		name      string
		count     int
		batchSize int
		want      [][]int
	}{
		{"one-at-a-time", 4, 1, [][]int{{0}, {1}, {2}, {3}}},
		{"even-split", 4, 2, [][]int{{0, 1}, {2, 3}}},
		{"uneven-last-batch", 5, 2, [][]int{{0, 1}, {2, 3}, {4}}},
		{"batch-larger-than-fleet", 3, 10, [][]int{{0, 1, 2}}},
		{"zero-means-whole-fleet", 3, 0, [][]int{{0, 1, 2}}},
		{"negative-means-whole-fleet", 3, -1, [][]int{{0, 1, 2}}},
		{"empty-fleet", 0, 2, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := batchClusterIDs(tc.count, tc.batchSize)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("batchClusterIDs(%d,%d) = %v, want %v", tc.count, tc.batchSize, got, tc.want)
			}
		})
	}
}

func TestClusterIDs(t *testing.T) {
	if got := clusterIDs(3); !reflect.DeepEqual(got, []int{0, 1, 2}) {
		t.Fatalf("clusterIDs(3) = %v", got)
	}
	if got := clusterIDs(0); len(got) != 0 {
		t.Fatalf("clusterIDs(0) = %v, want empty", got)
	}
}

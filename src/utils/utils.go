package utils

import (
	"fmt"
)

func FindFileNMaxAndMin(names []string, match string) (int, int) {
	var (
		fileN      int
		firstfound bool = true
		max        int
		min        int
	)
	for _, file := range names {
		_, err := fmt.Sscanf(file, match, &fileN)
		if err != nil {
			continue
		}
		if fileN > max {
			max = fileN
		} else if firstfound || fileN < min {
			min = fileN
			firstfound = false
		}
	}
	return max, min
}

package internal

import "github.com/ttab/elephant/doc"

func GetData(data doc.DataMap, key string) string {
	if data == nil {
		return ""
	}

	return data[key]
}

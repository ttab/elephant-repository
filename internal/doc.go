package internal

import "github.com/ttab/newsdoc"

func GetData(data newsdoc.DataMap, key string) string {
	if data == nil {
		return ""
	}

	return data[key]
}

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/plugins/serializers"
)

func main() {
	parser := influx.NewStreamParser(os.Stdin)
	serializer := serializers.NewInfluxSerializer()

	itemName := "items"

	for {
		metric, err := parser.Next()
		if err != nil {
			if err == influx.EOF {
				return // stream ended
			}
			if parseErr, isParseError := err.(*influx.ParseError); isParseError {
				fmt.Fprintf(os.Stderr, "parse ERR %v\n", parseErr)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "ERR %v\n", err)
			os.Exit(1)
		}

		c, found := metric.GetField(itemName)
		if !found {
			fmt.Fprintf(os.Stderr, "metric has no %s field\n", itemName)
			os.Exit(1)
		}
		switch t := c.(type) {
		case string:
			var items []interface{}
			if err := json.Unmarshal([]byte(t), &items); err != nil {
				fmt.Fprintf(os.Stderr, "ERR %v\\n", err)
				os.Exit(1)
			}
			metric.RemoveField(itemName)
			for _, values := range items {
				met := metric.Copy()
				switch vv := values.(type) {
				case map[string]interface{}:
					for k, v := range vv {
						met.AddField(k, v)
					}
				default:
					fmt.Fprintf(os.Stderr, "unknown type, it's a %T\n", values)
					os.Exit(1)
				}
				b, err := serializer.Serialize(met)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERR %v\n", err)
					os.Exit(1)
				}
				fmt.Fprint(os.Stdout, string(b))
			}
		default:
			fmt.Fprintf(os.Stderr, "%s is not an unknown type, it's a %T\n", itemName, c)
			os.Exit(1)
		}
	}
}

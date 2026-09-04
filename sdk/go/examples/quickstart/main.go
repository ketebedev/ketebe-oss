package main

import (
	"context"
	"fmt"
	ketebe "github.com/fatihbm/ketebe/sdk/go"
)

func main() {
	client, err := ketebe.NewClient(ketebe.ClientOptions{BaseURL: "http://127.0.0.1:17610"})
	if err != nil {
		panic(err)
	}
	collections, err := client.ListCollections(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(collections)
}

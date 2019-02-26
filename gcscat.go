package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"

	"cloud.google.com/go/storage"
)

func parseURI(uri string) (bucket string, object string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "gs" {
		return "", "", errors.New("invalid scheme: " + uri)
	}

	path := u.Path
	if path[0] == '/' {
		path = path[1:]
	}

	return u.Host, path, nil
}

func gcsCat(uri string, w io.Writer) error {
	bucket, object, err := parseURI(uri)
	if err != nil {
		return err
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	r, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Cannot read object: %v", err)
	}
	defer r.Close()

	_, err = io.Copy(w, r)
	return err
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: gcscat object_url\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()
	uri := flag.Arg(0)

	if uri == "" {
		usage()
		return
	}

	err := gcsCat(uri, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
}

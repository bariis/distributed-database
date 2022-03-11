package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	proto "github.com/huseyinbabal/demory-proto/golang/demory"
	_ "github.com/huseyinbabal/grpc-multi-resolver"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"time"
)

func main() {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)), //wait time is increased exponentially after every attempt
		grpc_retry.WithMax(5),
	}

	conn, connErr := grpc.Dial("multi:///localhost:8081,localhost:8082",
		grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)), // wait until the server side is ready
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if connErr != nil {
		log.Fatalf("connection error %v:", connErr)
	}

	defer conn.Close()

	c := proto.NewDemoryClient(conn)

	go func() {
		for i := 0; i < 1000; i++ {
			uuid, _ := uuid.NewRandom()
			_, putErr := c.MapPut(context.Background(), &proto.MapPutRequest{
				Key:   strconv.Itoa(i),
				Value: []byte(uuid.String()),
			})

			if putErr != nil {
				log.Fatalf("put error %v:", putErr)
			}
		}
	}()

	go func() {
		for i := 0; i < 10; i++ {
			value, valueErr := c.MapGet(context.Background(), &proto.MapGetRequest{Key: strconv.Itoa(i)})
			if valueErr != nil {
				return
			}

			fmt.Printf("value: %v", value)
			time.Sleep(time.Second)
		}
	}()

	var dummy string
	fmt.Scanf("%s", &dummy)
}


This is an example of using Protobuf with Mast. There are probably
better patterns but I'm going to experiment with having the caller
map the mast.Node into whatever Proto message they want.

To regenerate the Go files from v1/*.proto changes, you need:

```
go install github.com/bufbuild/buf/cmd/buf@v1.9.0
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```


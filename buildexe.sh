# no idea how this is supposed to work
if [ "$(uname)" == "Darwin" ]; then
    env GOOS=darwin GOARCH=amd64 go build main.go  
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    echo 'linux'
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW32_NT" ]; then
    env GOOS=windows GOARCH=amd64 go build main.go
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW64_NT" ]; then
    env GOOS=windows GOARCH=amd64 go build main.go
fi


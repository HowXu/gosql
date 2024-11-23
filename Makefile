.PHONY: all build run
.DEFAULT_GOAL := run
export MAKEFLAGS := -s
all: build run

build:
	if not exist build mkdir build
	go build -o ./build/gosql.exe ./main 

run: build
	if not exist run mkdir run
	copy .\build\gosql.exe .\run
	cd .\run &&.\gosql.exe $(ARGS)
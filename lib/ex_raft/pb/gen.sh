#!/bin/bash
rm -rf ./*.ex
protoc --elixir_out=./ ./*.proto
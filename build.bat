@echo off
setlocal
set CGO_ENABLED=0

echo === ARMv7 ===
set GOOS=linux
set GOARCH=arm
set GOARM=7
go build -ldflags="-s -w" -o mdv_gateway_arm7 mdv_gateway.go
echo OK

echo === ARM64 ===
set GOARCH=arm64
go build -ldflags="-s -w" -o mdv_gateway_arm64 mdv_gateway.go
echo OK

echo === x86_64 ===
set GOARCH=amd64
go build -ldflags="-s -w" -o mdv_gateway_amd64 mdv_gateway.go
echo OK


dir mdv_gateway_*
pause
endlocal
.PHONY: all clean

s:
	rm -f mr-out*
	go run mrcoordinator.go pg-*.txt

c:
	go build -buildmode=plugin ../mrapps/wc.go
	go run mrworker.go wc.so

clean:
	rm -f mr-out*
